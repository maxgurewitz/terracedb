use std::{collections::HashMap, str};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Symbol(pub u32);

#[derive(Debug, Deserialize, Serialize)]
pub struct SymbolTable {
    string_bytes: Vec<u8>,
    symbol_live: Vec<bool>,
    symbol_string_start: Vec<u32>,
    symbol_string_len: Vec<u32>,
    #[serde(skip, default)]
    by_name: HashMap<String, Symbol>,
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            string_bytes: Vec::new(),
            symbol_live: Vec::new(),
            symbol_string_start: Vec::new(),
            symbol_string_len: Vec::new(),
            by_name: HashMap::new(),
        }
    }

    pub fn intern(&mut self, name: &str) -> Symbol {
        self.rebuild_index_if_needed();

        if let Some(symbol) = self.by_name.get(name) {
            return *symbol;
        }

        let symbol = Symbol(self.symbol_live.len() as u32);
        let start = self.string_bytes.len() as u32;
        self.string_bytes.extend_from_slice(name.as_bytes());
        self.symbol_live.push(true);
        self.symbol_string_start.push(start);
        self.symbol_string_len.push(name.len() as u32);
        self.by_name.insert(name.to_owned(), symbol);
        symbol
    }

    pub fn resolve(&self, symbol: Symbol) -> Option<&str> {
        let index = symbol.0 as usize;
        if !self.symbol_live.get(index).copied().unwrap_or(false) {
            return None;
        }

        let start = self.symbol_string_start[index] as usize;
        let len = self.symbol_string_len[index] as usize;
        str::from_utf8(self.string_bytes.get(start..start + len)?).ok()
    }

    pub fn resolve_expect(&self, symbol: Symbol) -> &str {
        self.resolve(symbol).expect("invalid symbol")
    }

    fn rebuild_index_if_needed(&mut self) {
        if !self.by_name.is_empty() || self.symbol_live.is_empty() {
            return;
        }

        for index in 0..self.symbol_live.len() {
            if !self.symbol_live[index] {
                continue;
            }

            let symbol = Symbol(index as u32);
            if let Some(name) = self.resolve(symbol) {
                self.by_name.insert(name.to_owned(), symbol);
            }
        }
    }
}

impl Default for SymbolTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::SymbolTable;

    #[test]
    fn symbol_table_interns_same_name_to_same_symbol() {
        let mut symbols = SymbolTable::new();

        let a = symbols.intern("x");
        let b = symbols.intern("x");
        let c = symbols.intern("y");

        assert_eq!(a, b);
        assert_ne!(a, c);

        assert_eq!(symbols.resolve_expect(a), "x");
        assert_eq!(symbols.resolve_expect(c), "y");
    }
}
