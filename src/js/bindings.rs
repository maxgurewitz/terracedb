use std::collections::HashMap;

use crate::Error;

use super::{JsHeap, JsValue, Symbol, SymbolTable};

pub struct EnvStack {
    scopes: Vec<LexicalEnv>,
}

pub struct LexicalEnv {
    bindings: HashMap<Symbol, Binding>,
}

pub struct Binding {
    kind: BindingKind,
    value: JsValue,
    initialized: bool,
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

    fn declare(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        if self.bindings.contains_key(&name) {
            return Err(Error::JsDuplicateBinding {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        self.bindings.insert(
            name,
            Binding {
                kind,
                value: JsValue::Undefined,
                initialized: false,
            },
        );

        Ok(())
    }

    fn get(&self, name: Symbol) -> Option<&Binding> {
        self.bindings.get(&name)
    }

    fn get_mut(&mut self, name: Symbol) -> Option<&mut Binding> {
        self.bindings.get_mut(&name)
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

    pub(crate) fn pop_scope(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        if self.scopes.len() == 1 {
            return Err(Error::JsCannotPopRootScope);
        }

        let scope = self.scopes.pop().expect("scope length checked above");
        release_scope(scope, heap)?;

        Ok(())
    }

    pub(crate) fn depth(&self) -> usize {
        self.scopes.len()
    }

    pub(crate) fn truncate_to_depth(
        &mut self,
        depth: usize,
        heap: &mut JsHeap,
    ) -> Result<(), Error> {
        while self.scopes.len() > depth && self.scopes.len() > 1 {
            let scope = self.scopes.pop().expect("scope length checked above");
            release_scope(scope, heap)?;
        }

        Ok(())
    }

    pub(crate) fn declare_current(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.scopes
            .last_mut()
            .expect("env stack always has a root scope")
            .declare(name, kind, symbols)
    }

    pub fn declare_current_value(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.declare_current(name, kind, symbols)?;
        self.store(name, value, heap, symbols)
    }

    pub(crate) fn lookup(
        &self,
        name: Symbol,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        let value = self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .map(|binding| binding.value.clone())
            .ok_or_else(|| Error::JsBindingNotFound {
                name: symbols.resolve_expect(name).to_owned(),
            })?;

        heap.dup_value(&value);

        Ok(value)
    }

    pub(crate) fn store(
        &mut self,
        name: Symbol,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        for scope in self.scopes.iter_mut().rev() {
            let Some(binding) = scope.get_mut(name) else {
                continue;
            };

            if binding.kind == BindingKind::Const && binding.initialized {
                return Err(Error::JsAssignToConst {
                    name: symbols.resolve_expect(name).to_owned(),
                });
            }

            heap.dup_value(&value);
            let old = std::mem::replace(&mut binding.value, value);
            heap.free_value(old)?;
            binding.initialized = true;

            return Ok(());
        }

        Err(Error::JsBindingNotFound {
            name: symbols.resolve_expect(name).to_owned(),
        })
    }

    pub(crate) fn release_all_scopes(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        while let Some(scope) = self.scopes.pop() {
            release_scope(scope, heap)?;
        }

        Ok(())
    }
}

fn release_scope(scope: LexicalEnv, heap: &mut JsHeap) -> Result<(), Error> {
    for binding in scope.bindings.into_values() {
        heap.free_value(binding.value)?;
    }

    Ok(())
}
