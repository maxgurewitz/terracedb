use boa_ast::{
    Declaration, Expression, Span, Spanned, Statement, StatementListItem,
    declaration::{Binding, LexicalDeclaration, VariableList},
    expression::{
        access::{PropertyAccess, PropertyAccessField},
        literal::LiteralKind,
        operator::binary::{ArithmeticOp, BinaryOp},
    },
    scope::Scope,
};
use boa_interner::{Interner, ToInternedString};
use boa_parser::{Parser, Source};

use crate::Error;

use super::{MiniExpr, MiniProgram, MiniStmt};

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

pub(crate) fn parse_and_lower_minijs(source: &str) -> Result<MiniProgram, Error> {
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
        lower_statement_list_item(item, &interner, &mut program)?;
    }

    Ok(program)
}

fn lower_statement_list_item(
    item: &StatementListItem,
    interner: &Interner,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match item {
        StatementListItem::Declaration(declaration) => {
            lower_declaration(declaration, interner, program)
        }
        StatementListItem::Statement(statement) => lower_statement(statement, interner, program),
    }
}

fn lower_declaration(
    declaration: &Declaration,
    interner: &Interner,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match declaration {
        Declaration::Lexical(LexicalDeclaration::Let(list)) => {
            lower_let_declaration(list, interner, program)
        }
        Declaration::Lexical(LexicalDeclaration::Const(_)) => {
            unsupported("const declaration", JsSpan::unknown())
        }
        _ => unsupported("declaration", JsSpan::unknown()),
    }
}

fn lower_let_declaration(
    list: &VariableList,
    interner: &Interner,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    for variable in list.as_ref() {
        let name = match variable.binding() {
            Binding::Identifier(identifier) => identifier.to_interned_string(interner),
            Binding::Pattern(_) => {
                return unsupported("destructuring binding", JsSpan::unknown());
            }
        };

        let Some(expr) = variable.init() else {
            return unsupported("let without initializer", JsSpan::unknown());
        };

        program.push(MiniStmt::Let {
            name,
            expr: lower_expr(expr, interner)?,
        });
    }

    Ok(())
}

fn lower_statement(
    statement: &Statement,
    interner: &Interner,
    program: &mut MiniProgram,
) -> Result<(), Error> {
    match statement {
        Statement::Expression(expr) => {
            program.push(lower_expression_statement(expr, interner)?);
            Ok(())
        }
        Statement::Empty => Ok(()),
        _ => unsupported("statement", JsSpan::unknown()),
    }
}

fn lower_expression_statement(expr: &Expression, interner: &Interner) -> Result<MiniStmt, Error> {
    let expr = expr.flatten();

    let Expression::Call(call) = expr else {
        return unsupported("expression statement", span_of(expr));
    };

    if !is_console_method(call.function(), "log", interner) {
        return unsupported("expression statement", span_of(expr));
    }

    let [arg] = call.args() else {
        return unsupported("console.log arity", span_of(expr));
    };

    Ok(MiniStmt::ConsoleLog {
        expr: lower_expr(arg, interner)?,
    })
}

fn lower_expr(expr: &Expression, interner: &Interner) -> Result<MiniExpr, Error> {
    let expr = expr.flatten();

    match expr {
        Expression::Literal(literal) => match literal.kind() {
            LiteralKind::Num(value) => Ok(MiniExpr::Number(*value)),
            LiteralKind::Int(value) => Ok(MiniExpr::Number(f64::from(*value))),
            _ => unsupported("literal", span_of(expr)),
        },
        Expression::Identifier(identifier) => {
            Ok(MiniExpr::Ident(identifier.to_interned_string(interner)))
        }
        Expression::Binary(binary) => {
            if binary.op() != BinaryOp::Arithmetic(ArithmeticOp::Add) {
                return unsupported("binary operator", span_of(expr));
            }

            Ok(MiniExpr::Add(
                Box::new(lower_expr(binary.lhs(), interner)?),
                Box::new(lower_expr(binary.rhs(), interner)?),
            ))
        }
        _ => unsupported(expression_feature(expr), span_of(expr)),
    }
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
