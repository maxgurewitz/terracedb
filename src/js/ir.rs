pub(crate) type MiniProgram = Vec<MiniStmt>;

pub(crate) enum MiniStmt {
    Block(Vec<MiniStmt>),
    Let { name: String, expr: MiniExpr },
    Const { name: String, expr: MiniExpr },
    Assign { name: String, expr: MiniExpr },
    ConsoleLog { expr: MiniExpr },
}

pub(crate) enum MiniExpr {
    Number(f64),
    Ident(String),
    Add(Box<MiniExpr>, Box<MiniExpr>),
}
