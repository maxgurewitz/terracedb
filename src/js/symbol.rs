use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct Symbol(pub u32);

pub struct SymbolTable {
    names: Vec<String>,
    by_name: HashMap<String, Symbol>,
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            names: Vec::new(),
            by_name: HashMap::new(),
        }
    }

    pub fn intern(&mut self, name: &str) -> Symbol {
        if let Some(symbol) = self.by_name.get(name) {
            return *symbol;
        }

        let symbol = Symbol(self.names.len() as u32);
        let name = name.to_owned();

        self.names.push(name.clone());
        self.by_name.insert(name, symbol);

        symbol
    }

    pub fn resolve(&self, symbol: Symbol) -> Option<&str> {
        self.names.get(symbol.0 as usize).map(String::as_str)
    }

    pub fn resolve_expect(&self, symbol: Symbol) -> &str {
        self.resolve(symbol).expect("invalid symbol")
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
