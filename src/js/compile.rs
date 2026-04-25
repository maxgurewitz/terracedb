use boa_ast::{
    Declaration, Expression, Span, Spanned, Statement, StatementListItem,
    declaration::{Binding, LexicalDeclaration, VariableList},
    expression::{
        access::{PropertyAccess, PropertyAccessField},
        literal::{LiteralKind, ObjectLiteral, PropertyDefinition},
        operator::{
            assign::{AssignOp, AssignTarget},
            binary::{
                ArithmeticOp as BoaArithmeticOp, BinaryOp as BoaBinaryOp,
                LogicalOp as BoaLogicalOp, RelationalOp as BoaRelationalOp,
            },
            unary::UnaryOp as BoaUnaryOp,
        },
    },
    property::PropertyName,
    scope::Scope,
};
use boa_interner::{Interner, ToInternedString};
use boa_parser::{Parser, Source};

use crate::Error;

use super::{
    BindingKind, BytecodeProgram, ConstId, Constant, Instr, PropertyKey, Symbol, SymbolTable,
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

pub fn compile_source_to_bytecode(
    source: &str,
    symbols: &mut SymbolTable,
) -> Result<BytecodeProgram, Error> {
    let mut interner = Interner::default();
    let scope = Scope::default();
    let mut parser = Parser::new(Source::from_bytes(source));
    let script = parser.parse_script(&scope, &mut interner).map_err(|err| {
        Error::JsCompile(JsCompileError::Parse {
            message: err.to_string(),
        })
    })?;

    let mut compiler = Compiler::new();

    for item in script.statements().statements() {
        compiler.compile_statement_list_item(item, &interner, symbols)?;
    }

    compiler.emit(Instr::Halt);

    Ok(compiler.finish())
}

struct Compiler {
    program: BytecodeProgram,
}

impl Compiler {
    fn new() -> Self {
        Self {
            program: BytecodeProgram::new(),
        }
    }

    fn finish(self) -> BytecodeProgram {
        self.program
    }

    fn emit(&mut self, instr: Instr) -> usize {
        let at = self.current_ip();
        self.program.instructions.push(instr);
        at
    }

    fn emit_const(&mut self, constant: Constant) -> ConstId {
        self.program.constants.push(constant)
    }

    fn emit_load_const(&mut self, constant: Constant) {
        let id = self.emit_const(constant);
        self.emit(Instr::LoadConst(id));
    }

    fn emit_load_bool(&mut self, value: bool) {
        self.emit_load_const(Constant::Bool(value));
    }

    fn current_ip(&self) -> usize {
        self.program.instructions.len()
    }

    fn patch_jump(&mut self, at: usize, target: usize) {
        match self
            .program
            .instructions
            .get_mut(at)
            .expect("invalid jump patch")
        {
            Instr::Jump(slot) | Instr::JumpIfFalse(slot) | Instr::JumpIfTrue(slot) => {
                *slot = target;
            }
            _ => panic!("cannot patch non-jump instruction"),
        }
    }

    fn compile_statement_list_item(
        &mut self,
        item: &StatementListItem,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        match item {
            StatementListItem::Declaration(declaration) => {
                self.compile_declaration(declaration, interner, symbols)
            }
            StatementListItem::Statement(statement) => {
                self.compile_statement(statement, interner, symbols)
            }
        }
    }

    fn compile_declaration(
        &mut self,
        declaration: &Declaration,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        match declaration {
            Declaration::Lexical(LexicalDeclaration::Let(list)) => {
                self.compile_lexical_declaration(list, BindingKind::Let, interner, symbols)
            }
            Declaration::Lexical(LexicalDeclaration::Const(list)) => {
                self.compile_lexical_declaration(list, BindingKind::Const, interner, symbols)
            }
            _ => unsupported("declaration", JsSpan::unknown()),
        }
    }

    fn compile_lexical_declaration(
        &mut self,
        list: &VariableList,
        kind: BindingKind,
        interner: &Interner,
        symbols: &mut SymbolTable,
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

            self.compile_expr(expr, interner, symbols)?;

            match kind {
                BindingKind::Let => self.emit(Instr::DeclareLet(name)),
                BindingKind::Const => self.emit(Instr::DeclareConst(name)),
            };

            self.emit(Instr::StoreBinding(name));
        }

        Ok(())
    }

    fn compile_statement(
        &mut self,
        statement: &Statement,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        match statement {
            Statement::Block(block) => {
                self.emit(Instr::PushScope);

                for item in block.statement_list().statements() {
                    self.compile_statement_list_item(item, interner, symbols)?;
                }

                self.emit(Instr::PopScope);

                Ok(())
            }
            Statement::Expression(expr) => {
                self.compile_expression_statement(expr, interner, symbols)
            }
            Statement::Empty => Ok(()),
            _ => unsupported("statement", JsSpan::unknown()),
        }
    }

    fn compile_expression_statement(
        &mut self,
        expr: &Expression,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        let expr = expr.flatten();

        if let Expression::Assign(assign) = expr {
            self.compile_assignment(assign, interner, symbols, false)?;
            return Ok(());
        }

        if let Expression::Call(call) = expr {
            if !is_console_method(call.function(), "log", interner) {
                return unsupported("expression statement", span_of(expr));
            }

            let [arg] = call.args() else {
                return unsupported("console.log arity", span_of(expr));
            };

            self.compile_expr(arg, interner, symbols)?;
            self.emit(Instr::ConsoleLog);

            return Ok(());
        }

        self.compile_expr(expr, interner, symbols)
    }

    fn compile_expr(
        &mut self,
        expr: &Expression,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        let expr = expr.flatten();

        match expr {
            Expression::Literal(literal) => match literal.kind() {
                LiteralKind::Num(value) => self.emit_load_const(Constant::Number(*value)),
                LiteralKind::Int(value) => {
                    self.emit_load_const(Constant::Number(f64::from(*value)));
                }
                LiteralKind::Bool(value) => self.emit_load_const(Constant::Bool(*value)),
                LiteralKind::Null => self.emit_load_const(Constant::Null),
                LiteralKind::Undefined => self.emit_load_const(Constant::Undefined),
                LiteralKind::String(symbol) => {
                    self.emit_load_const(Constant::String(
                        interner.resolve_expect(*symbol).to_string(),
                    ));
                }
                _ => return unsupported("literal", span_of(expr)),
            },
            Expression::Identifier(identifier) => {
                if identifier.to_interned_string(interner) == "undefined" {
                    self.emit_load_const(Constant::Undefined);
                } else {
                    self.emit(Instr::LoadBinding(lower_identifier(
                        *identifier,
                        interner,
                        symbols,
                    )));
                }
            }
            Expression::Assign(assign) => {
                self.compile_assignment(assign, interner, symbols, true)?;
            }
            Expression::ObjectLiteral(object) => {
                self.compile_object_literal(object, interner, symbols)?;
            }
            Expression::PropertyAccess(access) => {
                self.compile_property_access(access, interner, symbols)?;
            }
            Expression::Unary(unary) => {
                if unary.op() != BoaUnaryOp::Not {
                    return unsupported("unary operator", span_of(expr));
                }

                self.compile_expr(unary.target(), interner, symbols)?;
                self.emit(Instr::LogicalNot);
            }
            Expression::Binary(binary) => {
                self.compile_binary(binary.op(), binary.lhs(), binary.rhs(), interner, symbols)?;
            }
            _ => return unsupported(expression_feature(expr), span_of(expr)),
        }

        Ok(())
    }

    fn compile_assignment(
        &mut self,
        assign: &boa_ast::expression::operator::Assign,
        interner: &Interner,
        symbols: &mut SymbolTable,
        leave_value: bool,
    ) -> Result<(), Error> {
        if assign.op() != AssignOp::Assign {
            return unsupported("compound assignment", span_of(assign));
        }

        match assign.lhs() {
            AssignTarget::Identifier(identifier) => {
                let name = lower_identifier(*identifier, interner, symbols);

                self.compile_expr(assign.rhs(), interner, symbols)?;
                self.emit(Instr::StoreBinding(name));

                if leave_value {
                    self.emit(Instr::LoadBinding(name));
                }
            }
            AssignTarget::Access(access) => {
                let (target, key) = lower_property_access(access, interner, symbols)?;

                self.compile_expr(target, interner, symbols)?;
                self.compile_expr(assign.rhs(), interner, symbols)?;
                self.emit(Instr::SetProperty(key));
            }
            AssignTarget::Pattern(_) => {
                return unsupported("assignment pattern", span_of(assign));
            }
        }

        Ok(())
    }

    fn compile_object_literal(
        &mut self,
        object: &ObjectLiteral,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        self.emit(Instr::NewObject);

        for property in object.properties() {
            let PropertyDefinition::Property(name, expr) = property else {
                return unsupported("object literal property", JsSpan::unknown());
            };

            let key = lower_property_name(name, interner, symbols)?;
            self.compile_expr(expr, interner, symbols)?;
            self.emit(Instr::DefineProperty(key));
        }

        Ok(())
    }

    fn compile_property_access(
        &mut self,
        access: &PropertyAccess,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        let (target, key) = lower_property_access(access, interner, symbols)?;

        self.compile_expr(target, interner, symbols)?;
        self.emit(Instr::GetProperty(key));

        Ok(())
    }

    fn compile_binary(
        &mut self,
        op: BoaBinaryOp,
        lhs: &Expression,
        rhs: &Expression,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        match op {
            BoaBinaryOp::Logical(BoaLogicalOp::And) => {
                self.compile_logical_and(lhs, rhs, interner, symbols)
            }
            BoaBinaryOp::Logical(BoaLogicalOp::Or) => {
                self.compile_logical_or(lhs, rhs, interner, symbols)
            }
            _ => {
                self.compile_expr(lhs, interner, symbols)?;
                self.compile_expr(rhs, interner, symbols)?;
                self.emit(lower_binary_instr(op)?);
                Ok(())
            }
        }
    }

    fn compile_logical_and(
        &mut self,
        lhs: &Expression,
        rhs: &Expression,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        self.compile_expr(lhs, interner, symbols)?;
        let left_false = self.emit(Instr::JumpIfFalse(usize::MAX));

        self.compile_expr(rhs, interner, symbols)?;
        let right_false = self.emit(Instr::JumpIfFalse(usize::MAX));

        self.emit_load_bool(true);
        let end_jump = self.emit(Instr::Jump(usize::MAX));

        let false_label = self.current_ip();
        self.patch_jump(left_false, false_label);
        self.patch_jump(right_false, false_label);
        self.emit_load_bool(false);

        let end_label = self.current_ip();
        self.patch_jump(end_jump, end_label);

        Ok(())
    }

    fn compile_logical_or(
        &mut self,
        lhs: &Expression,
        rhs: &Expression,
        interner: &Interner,
        symbols: &mut SymbolTable,
    ) -> Result<(), Error> {
        self.compile_expr(lhs, interner, symbols)?;
        let left_true = self.emit(Instr::JumpIfTrue(usize::MAX));

        self.compile_expr(rhs, interner, symbols)?;
        let right_true = self.emit(Instr::JumpIfTrue(usize::MAX));

        self.emit_load_bool(false);
        let end_jump = self.emit(Instr::Jump(usize::MAX));

        let true_label = self.current_ip();
        self.patch_jump(left_true, true_label);
        self.patch_jump(right_true, true_label);
        self.emit_load_bool(true);

        let end_label = self.current_ip();
        self.patch_jump(end_jump, end_label);

        Ok(())
    }
}

fn lower_binary_instr(op: BoaBinaryOp) -> Result<Instr, Error> {
    let instr = match op {
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Add) => Instr::Add,
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Sub) => Instr::Sub,
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Mul) => Instr::Mul,
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Div) => Instr::Div,
        BoaBinaryOp::Arithmetic(BoaArithmeticOp::Mod) => Instr::Mod,
        BoaBinaryOp::Relational(BoaRelationalOp::LessThan) => Instr::LessThan,
        BoaBinaryOp::Relational(BoaRelationalOp::LessThanOrEqual) => Instr::LessThanOrEqual,
        BoaBinaryOp::Relational(BoaRelationalOp::GreaterThan) => Instr::GreaterThan,
        BoaBinaryOp::Relational(BoaRelationalOp::GreaterThanOrEqual) => Instr::GreaterThanOrEqual,
        BoaBinaryOp::Relational(BoaRelationalOp::StrictEqual) => Instr::StrictEqual,
        BoaBinaryOp::Relational(BoaRelationalOp::StrictNotEqual) => Instr::StrictNotEqual,
        _ => return unsupported("binary operator", JsSpan::unknown()),
    };

    Ok(instr)
}

fn lower_identifier(
    identifier: boa_ast::expression::Identifier,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Symbol {
    let name = identifier.to_interned_string(interner);

    symbols.intern(&name)
}

fn lower_property_name(
    name: &PropertyName,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Result<PropertyKey, Error> {
    let PropertyName::Literal(identifier) = name else {
        return unsupported("computed property name", JsSpan::unknown());
    };

    Ok(PropertyKey::Symbol(lower_identifier(
        *identifier,
        interner,
        symbols,
    )))
}

fn lower_property_access<'a>(
    access: &'a PropertyAccess,
    interner: &Interner,
    symbols: &mut SymbolTable,
) -> Result<(&'a Expression, PropertyKey), Error> {
    let PropertyAccess::Simple(access) = access else {
        return unsupported("property access", span_of(access));
    };

    let PropertyAccessField::Const(field) = access.field() else {
        return unsupported("computed property access", span_of(access));
    };

    let key = PropertyKey::Symbol(lower_identifier(*field, interner, symbols));

    Ok((access.target(), key))
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

#[cfg(test)]
mod tests {
    use super::compile_source_to_bytecode;
    use crate::{Instr, SymbolTable};

    #[test]
    fn compiler_emits_bytecode_for_let_and_console_log() {
        let mut symbols = SymbolTable::new();
        let program = compile_source_to_bytecode(
            r#"
                let x = 1;
                console.log(x);
            "#,
            &mut symbols,
        )
        .unwrap();

        assert!(
            program
                .instructions
                .iter()
                .any(|instr| matches!(instr, Instr::DeclareLet(_)))
        );
        assert!(
            program
                .instructions
                .iter()
                .any(|instr| matches!(instr, Instr::ConsoleLog))
        );
        assert!(matches!(program.instructions.last(), Some(Instr::Halt)));
    }
}
