use std::collections::HashMap;

use crate::Error;

use super::JsValue;

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
    pub(crate) fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    pub(crate) fn declare(
        &mut self,
        name: String,
        kind: BindingKind,
        value: JsValue,
    ) -> Result<(), Error> {
        if self.bindings.contains_key(&name) {
            return Err(Error::JsBindingAlreadyDeclared { name });
        }

        self.bindings.insert(name, Binding { kind, value });

        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Result<JsValue, Error> {
        self.bindings
            .get(name)
            .map(|binding| binding.value.clone())
            .ok_or_else(|| Error::JsBindingNotFound {
                name: name.to_owned(),
            })
    }

    pub(crate) fn assign(&mut self, name: &str, value: JsValue) -> Result<(), Error> {
        let binding = self
            .bindings
            .get_mut(name)
            .ok_or_else(|| Error::JsBindingNotFound {
                name: name.to_owned(),
            })?;

        if binding.kind == BindingKind::Const {
            return Err(Error::JsAssignToConst {
                name: name.to_owned(),
            });
        }

        binding.value = value;

        Ok(())
    }
}
