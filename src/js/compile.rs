use boa_ast::{
    Declaration, Expression, Span, Spanned, Statement, StatementListItem,
    declaration::{Binding, LexicalDeclaration, VariableList},
    expression::{
        access::{PropertyAccess, PropertyAccessField},
        literal::LiteralKind,
        operator::{
            assign::{AssignOp, AssignTarget},
            binary::{
                ArithmeticOp as BoaArithmeticOp, BinaryOp as BoaBinaryOp,
                LogicalOp as BoaLogicalOp, RelationalOp as BoaRelationalOp,
            },
            unary::UnaryOp as BoaUnaryOp,
        },
    },
    scope::Scope,
};
use boa_interner::{Interner, ToInternedString};
use boa_parser::{Parser, Source};

use crate::Error;

use super::{
    BinaryOp, BindingKind, LogicalOp, MiniExpr, MiniProgram, MiniStmt, Symbol, SymbolTable, UnaryOp,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsSpan {
    pub start_line: u32,
    pub start_column: u32,
    pub end_line: u32,
    pub end_column: u32,
}

impl JsSpan {
    pub const fn unknown() -> Self {
        Self {
            start_line: 1,
            start_column: 1,
            end_line: 1,
            end_column: 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsCompileError {
    Parse { message: String },
    UnsupportedSyntax { feature: &'static str, span: JsSpan },
}

pub(crate) fn parse_and_lower_minijs(
    source: &str,
    symbols: &mut SymbolTable,
) -> Result<MiniProgram, Error> {
    let mut interner = Interner::default();
    let scope = Scope::default();
    let mut parser = Parser::new(Source::from_bytes(source));
    let script = parser.parse_script(&scope, &mut interner).map_err(|err| {
        Error::JsCompile(JsCompileError::Parse {
            message: err.to_string(),
        })
    })?;

    let mut program = Vec::new();

    for item in script.statements().statements() {
        lower_statement_list_item(item, &interner, symbols, &mut program)?;
    }

    Ok(program)
}

fn lower_statement_list_item(
    item: &StatementListItem,
    interner: &Interner,
    symbols: &mut SymbolTable,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match item {
        StatementListItem::Declaration(declaration) => {
            lower_declaration(declaration, interner, symbols, program)
        }
        StatementListItem::Statement(statement) => {
            lower_statement(statement, interner, symbols, program)
        }
    }
}

fn lower_declaration(
    declaration: &Declaration,
    interner: &Interner,
    symbols: &mut SymbolTable,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match declaration {
        Declaration::Lexical(LexicalDeclaration::Let(list)) => {
            lower_lexical_declaration(list, BindingKind::Let, interner, symbols, program)
        }
        Declaration::Lexical(LexicalDeclaration::Const(list)) => {
            lower_lexical_declaration(list, BindingKind::Const, interner, symbols, program)
        }
        _ => unsupported("declaration", JsSpan::unknown()),
    }
}

fn lower_lexical_declaration(
    list: &VariableList,
    kind: BindingKind,
    interner: &Interner,
    symbols: &mut SymbolTable,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    for variable in list.as_ref() {
        let name = match variable.binding() {
            Binding::Identifier(identifier) => lower_identifier(*identifier, interner, symbols),
            Binding::Pattern(_) => {
                return unsupported("destructuring binding", JsSpan::unknown());
            }
        };

        let Some(expr) = variable.init() else {
            return unsupported("lexical declaration without initializer", JsSpan::unknown());
        };

        let expr = lower_expr(expr, interner, symbols)?;

        program.push(match kind {
            BindingKind::Let => MiniStmt::Let { name, expr },
            BindingKind::Const => MiniStmt::Const { name, expr },
        });
    }

    Ok(())
}

fn lower_statement(
    statement: &Statement,
    interner: &Interner,
    symbols: &mut SymbolTable,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match statement {
        Statement::Block(block) => {
            let mut body = Vec::new();

            for item in block.statement_list().statements() {
                lower_statement_list_item(item, interner, symbols, &mut body)?;
            }

            program.push(MiniStmt::Block(body));
            Ok(())
        }
        Statement::Expression(expr) => {
            program.push(lower_expression_statement(expr, interner, symbols)?);
            Ok(())
        }
        Statement::Empty => Ok(()),
        _ => unsupported("statement", JsSpan::unknown()),
    }
}

fn lower_expression_statement(
    expr: &Expression,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Result<MiniStmt, Error> {
    let expr = expr.flatten();

    if let Expression::Assign(assign) = expr {
        if assign.op() != AssignOp::Assign {
            return unsupported("compound assignment", span_of(expr));
        }

        let AssignTarget::Identifier(identifier) = assign.lhs() else {
            return unsupported("assignment target", span_of(expr));
        };

        return Ok(MiniStmt::Assign {
            name: lower_identifier(*identifier, interner, symbols),
            expr: lower_expr(assign.rhs(), interner, symbols)?,
        });
    }

    if let Expression::Call(call) = expr {
        if !is_console_method(call.function(), "log", interner) {
            return unsupported("expression statement", span_of(expr));
        }

        let [arg] = call.args() else {
            return unsupported("console.log arity", span_of(expr));
        };

        return Ok(MiniStmt::ConsoleLog {
            expr: lower_expr(arg, interner, symbols)?,
        });
    }

    Ok(MiniStmt::Expr(lower_expr(expr, interner, symbols)?))
}

fn lower_expr(
    expr: &Expression,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Result<MiniExpr, Error> {
    let expr = expr.flatten();

    match expr {
        Expression::Literal(literal) => match literal.kind() {
            LiteralKind::Num(value) => Ok(MiniExpr::Number(*value)),
            LiteralKind::Int(value) => Ok(MiniExpr::Number(f64::from(*value))),
            LiteralKind::Bool(value) => Ok(MiniExpr::Bool(*value)),
            LiteralKind::Null => Ok(MiniExpr::Null),
            LiteralKind::Undefined => Ok(MiniExpr::Undefined),
            LiteralKind::String(symbol) => Ok(MiniExpr::String(
                interner.resolve_expect(*symbol).to_string(),
            )),
            _ => unsupported("literal", span_of(expr)),
        },
        Expression::Identifier(identifier) => {
            if identifier.to_interned_string(interner) == "undefined" {
                Ok(MiniExpr::Undefined)
            } else {
                Ok(MiniExpr::Ident(lower_identifier(
                    *identifier,
                    interner,
                    symbols,
                )))
            }
        }
        Expression::Assign(assign) => {
            if assign.op() != AssignOp::Assign {
                return unsupported("compound assignment", span_of(expr));
            }

            let AssignTarget::Identifier(identifier) = assign.lhs() else {
                return unsupported("assignment target", span_of(expr));
            };

            Ok(MiniExpr::Assign {
                name: lower_identifier(*identifier, interner, symbols),
                expr: Box::new(lower_expr(assign.rhs(), interner, symbols)?),
            })
        }
        Expression::Unary(unary) => {
            if unary.op() != BoaUnaryOp::Not {
                return unsupported("unary operator", span_of(expr));
            }

            Ok(MiniExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(lower_expr(unary.target(), interner, symbols)?),
            })
        }
        Expression::Binary(binary) => {
            let left = Box::new(lower_expr(binary.lhs(), interner, symbols)?);
            let right = Box::new(lower_expr(binary.rhs(), interner, symbols)?);

            match lower_binary_op(binary.op())? {
                LoweredBinaryOp::Binary(op) => Ok(MiniExpr::Binary { op, left, right }),
                LoweredBinaryOp::Logical(op) => Ok(MiniExpr::Logical { op, left, right }),
            }
        }
        _ => unsupported(expression_feature(expr), span_of(expr)),
    }
}

enum LoweredBinaryOp {
    Binary(BinaryOp),
    Logical(LogicalOp),
}

fn lower_binary_op(op: BoaBinaryOp) -> Result<LoweredBinaryOp, Error> {
    let op = match op {
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Add) => LoweredBinaryOp::Binary(BinaryOp::Add),
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Sub) => LoweredBinaryOp::Binary(BinaryOp::Sub),
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Mul) => LoweredBinaryOp::Binary(BinaryOp::Mul),
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Div) => LoweredBinaryOp::Binary(BinaryOp::Div),
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Mod) => LoweredBinaryOp::Binary(BinaryOp::Mod),
        BoaBinaryOp::Relational(BoaRelationalOp::LessThan) => {
            LoweredBinaryOp::Binary(BinaryOp::LessThan)
        }
        BoaBinaryOp::Relational(BoaRelationalOp::LessThanOrEqual) => {
            LoweredBinaryOp::Binary(BinaryOp::LessThanOrEqual)
        }
        BoaBinaryOp::Relational(BoaRelationalOp::GreaterThan) => {
            LoweredBinaryOp::Binary(BinaryOp::GreaterThan)
        }
        BoaBinaryOp::Relational(BoaRelationalOp::GreaterThanOrEqual) => {
            LoweredBinaryOp::Binary(BinaryOp::GreaterThanOrEqual)
        }
        BoaBinaryOp::Relational(BoaRelationalOp::StrictEqual) => {
            LoweredBinaryOp::Binary(BinaryOp::StrictEqual)
        }
        BoaBinaryOp::Relational(BoaRelationalOp::StrictNotEqual) => {
            LoweredBinaryOp::Binary(BinaryOp::StrictNotEqual)
        }
        BoaBinaryOp::Logical(BoaLogicalOp::And) => LoweredBinaryOp::Logical(LogicalOp::And),
        BoaBinaryOp::Logical(BoaLogicalOp::Or) => LoweredBinaryOp::Logical(LogicalOp::Or),
        _ => return unsupported("binary operator", JsSpan::unknown()),
    };

    Ok(op)
}

fn lower_identifier(
    identifier: boa_ast::expression::Identifier,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Symbol {
    let name = identifier.to_interned_string(interner);

    symbols.intern(&name)
}

fn is_console_method(expr: &Expression, method: &str, interner: &Interner) -> bool {
    let Expression::PropertyAccess(PropertyAccess::Simple(access)) = expr.flatten() else {
        return false;
    };

    let Expression::Identifier(target) = access.target().flatten() else {
        return false;
    };

    if target.to_interned_string(interner) != "console" {
        return false;
    }

    let PropertyAccessField::Const(field) = access.field() else {
        return false;
    };

    field.to_interned_string(interner) == method
}

fn expression_feature(expr: &Expression) -> &'static str {
    match expr {
        Expression::This(_) => "this expression",
        Expression::Identifier(_) => "identifier",
        Expression::Literal(_) => "literal",
        Expression::RegExpLiteral(_) => "regexp literal",
        Expression::ArrayLiteral(_) => "array literal",
        Expression::ObjectLiteral(_) => "object literal",
        Expression::Spread(_) => "spread expression",
        Expression::FunctionExpression(_) => "function expression",
        Expression::ArrowFunction(_) => "arrow function",
        Expression::AsyncArrowFunction(_) => "async arrow function",
        Expression::GeneratorExpression(_) => "generator expression",
        Expression::AsyncFunctionExpression(_) => "async function expression",
        Expression::AsyncGeneratorExpression(_) => "async generator expression",
        Expression::ClassExpression(_) => "class expression",
        Expression::TemplateLiteral(_) => "template literal",
        Expression::PropertyAccess(_) => "property access",
        Expression::New(_) => "new expression",
        Expression::Call(_) => "call expression",
        Expression::SuperCall(_) => "super call",
        Expression::ImportCall(_) => "import call",
        Expression::Optional(_) => "optional expression",
        Expression::TaggedTemplate(_) => "tagged template",
        Expression::NewTarget(_) => "new.target",
        Expression::ImportMeta(_) => "import.meta",
        Expression::Assign(_) => "assignment",
        Expression::Unary(_) => "unary operator",
        Expression::Update(_) => "update operator",
        Expression::Binary(_) => "binary operator",
        Expression::BinaryInPrivate(_) => "private binary operator",
        Expression::Conditional(_) => "conditional expression",
        Expression::Await(_) => "await expression",
        Expression::Yield(_) => "yield expression",
        Expression::Parenthesized(_) => "parenthesized expression",
        Expression::FormalParameterList(_) => "formal parameter list",
        Expression::Debugger => "debugger",
    }
}

fn unsupported<T>(feature: &'static str, span: JsSpan) -> Result<T, Error> {
    Err(Error::JsCompile(JsCompileError::UnsupportedSyntax {
        feature,
        span,
    }))
}

fn span_of(item: &impl Spanned) -> JsSpan {
    from_boa_span(item.span())
}

fn from_boa_span(span: Span) -> JsSpan {
    JsSpan {
        start_line: span.start().line_number(),
        start_column: span.start().column_number(),
        end_line: span.end().line_number(),
        end_column: span.end().column_number(),
    }
}
