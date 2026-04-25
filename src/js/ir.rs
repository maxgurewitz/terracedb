use super::Symbol;

pub(crate) type MiniProgram = Vec<MiniStmt>;

pub(crate) enum MiniStmt {
    Block(Vec<MiniStmt>),
    Let { name: Symbol, expr: MiniExpr },
    Const { name: Symbol, expr: MiniExpr },
    Assign { name: Symbol, expr: MiniExpr },
    ConsoleLog { expr: MiniExpr },
}

pub(crate) enum MiniExpr {
    Number(f64),
    Ident(Symbol),
    Add(Box<MiniExpr>, Box<MiniExpr>),
}
