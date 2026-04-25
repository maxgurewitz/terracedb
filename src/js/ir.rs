use super::Symbol;

pub(crate) type MiniProgram = Vec<MiniStmt>;

pub(crate) enum MiniStmt {
    Block(Vec<MiniStmt>),
    Let { name: Symbol, expr: MiniExpr },
    Const { name: Symbol, expr: MiniExpr },
    Assign { name: Symbol, expr: MiniExpr },
    Expr(MiniExpr),
    ConsoleLog { expr: MiniExpr },
}

pub(crate) enum MiniExpr {
    Number(f64),
    Bool(bool),
    String(String),
    Null,
    Undefined,
    Ident(Symbol),
    Assign {
        name: Symbol,
        expr: Box<MiniExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<MiniExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<MiniExpr>,
        right: Box<MiniExpr>,
    },
    Logical {
        op: LogicalOp,
        left: Box<MiniExpr>,
        right: Box<MiniExpr>,
    },
}

pub(crate) enum UnaryOp {
    Not,
}

pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    StrictEqual,
    StrictNotEqual,
}

pub(crate) enum LogicalOp {
    And,
    Or,
}
