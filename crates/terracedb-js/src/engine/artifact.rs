use std::{collections::BTreeMap, path::Path};

use boa_ast::{
    Declaration, Expression, ModuleItem, ModuleItemList, Spanned, Statement, StatementListItem,
    declaration::{
        Binding, ExportDeclaration, ImportDeclaration, ImportKind, LexicalDeclaration, Variable,
        VariableList,
    },
    expression::{
        Call, Identifier,
        access::{PropertyAccess, PropertyAccessField},
        literal::{Literal, LiteralKind, PropertyDefinition},
        operator::assign::{AssignOp, AssignTarget},
        operator::binary::{ArithmeticOp, BinaryOp, LogicalOp, RelationalOp},
        operator::unary::UnaryOp,
        operator::{Assign, Binary, Unary},
    },
    function::{
        ArrowFunction, AsyncArrowFunction, FormalParameter, FunctionBody, FunctionExpression,
    },
    property::PropertyName,
    scope::Scope,
};
use boa_interner::Interner;
use boa_parser::{Parser, Source};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct JsCodeBlockId(pub u64);

impl JsCodeBlockId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsSourceSpan {
    pub start_line: u32,
    pub start_column: u32,
    pub end_line: u32,
    pub end_column: u32,
}

impl Default for JsSourceSpan {
    fn default() -> Self {
        Self {
            start_line: 1,
            start_column: 1,
            end_line: 1,
            end_column: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsCompiledArtifactKind {
    Script,
    Module,
    SyntheticModule,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsBindingKind {
    Var,
    Let,
    Const,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsUnaryOperator {
    Plus,
    Minus,
    Not,
    TypeOf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsBinaryOperator {
    Add,
    StrictEq,
    StrictNe,
    Eq,
    Ne,
    LogicalAnd,
    LogicalOr,
    NullishCoalesce,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsCompiledImport {
    pub request: String,
    pub default_binding: Option<String>,
    pub namespace_binding: Option<String>,
    pub named_bindings: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsCompiledExport {
    pub exported_name: String,
    pub local_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsCompiledCodeBlock {
    pub id: JsCodeBlockId,
    pub label: String,
    pub parameters: Vec<String>,
    pub instructions: Vec<JsInstruction>,
    pub instruction_spans: Vec<JsSourceSpan>,
    pub async_function: bool,
    pub strict: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsCompiledArtifact {
    pub specifier: String,
    pub kind: JsCompiledArtifactKind,
    pub source: String,
    pub strict: bool,
    pub main: JsCodeBlockId,
    pub code_blocks: BTreeMap<JsCodeBlockId, JsCompiledCodeBlock>,
    pub imports: Vec<JsCompiledImport>,
    pub exports: Vec<JsCompiledExport>,
    pub requests: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum JsInstruction {
    PushUndefined,
    PushNull,
    PushBool {
        value: bool,
    },
    PushNumber {
        value: f64,
    },
    PushString {
        value: String,
    },
    PushBuiltin {
        name: String,
    },
    LoadName {
        name: String,
    },
    DeclareName {
        name: String,
        binding: JsBindingKind,
    },
    StoreName {
        name: String,
    },
    Pop,
    Dup,
    CreateObject,
    CreateArray {
        element_count: usize,
    },
    DefineProperty {
        name: String,
    },
    GetProperty {
        name: String,
    },
    GetPropertyDynamic,
    SetProperty {
        name: String,
    },
    SetPropertyDynamic,
    CreateFunction {
        code_block: JsCodeBlockId,
        async_function: bool,
    },
    Call {
        argc: usize,
    },
    CallMethod {
        name: String,
        argc: usize,
    },
    Await,
    TypeOf,
    Unary {
        op: JsUnaryOperator,
    },
    Binary {
        op: JsBinaryOperator,
    },
    Return,
}

#[derive(Debug, Error)]
pub enum JsCompileError {
    #[error("parse failed for {specifier}: {message}")]
    Parse { specifier: String, message: String },
    #[error("unsupported syntax in {specifier}: {detail}")]
    UnsupportedSyntax { specifier: String, detail: String },
}

struct JsArtifactCompiler {
    specifier: String,
    source: String,
    interner: Interner,
    next_code_block_id: u64,
    code_blocks: BTreeMap<JsCodeBlockId, JsCompiledCodeBlock>,
}

impl JsArtifactCompiler {
    fn new(specifier: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            specifier: specifier.into(),
            source: source.into(),
            interner: Interner::default(),
            next_code_block_id: 1,
            code_blocks: BTreeMap::new(),
        }
    }

    fn compile_script(mut self) -> Result<JsCompiledArtifact, JsCompileError> {
        let scope = Scope::new_global();
        let source = Source::from_reader(
            self.source.as_bytes(),
            Some(Path::new(self.specifier.as_str())),
        );
        let (script, _) = Parser::new(source)
            .parse_script_with_source(&scope, &mut self.interner)
            .map_err(|error| JsCompileError::Parse {
                specifier: self.specifier.clone(),
                message: error.to_string(),
            })?;
        let strict = script.strict();
        let main = self.compile_statement_list(
            "<script>",
            script.statements(),
            strict,
            false,
            None,
            None,
        )?;
        Ok(JsCompiledArtifact {
            specifier: self.specifier,
            kind: JsCompiledArtifactKind::Script,
            source: self.source,
            strict,
            main,
            code_blocks: self.code_blocks,
            imports: Vec::new(),
            exports: Vec::new(),
            requests: Vec::new(),
            metadata: BTreeMap::new(),
        })
    }

    fn compile_module(mut self) -> Result<JsCompiledArtifact, JsCompileError> {
        let scope = Scope::new_global();
        let source = Source::from_reader(
            self.source.as_bytes(),
            Some(Path::new(self.specifier.as_str())),
        );
        let (module, _) = Parser::new(source)
            .parse_module_with_source(&scope, &mut self.interner)
            .map_err(|error| JsCompileError::Parse {
                specifier: self.specifier.clone(),
                message: error.to_string(),
            })?;

        let requests = module
            .items()
            .requests()
            .into_iter()
            .map(|sym| self.symbol(sym))
            .collect::<Vec<_>>();
        let imports = self.compile_module_imports(module.items())?;
        let exports = self.compile_module_exports(module.items())?;
        let main = self.compile_module_items(module.items())?;
        Ok(JsCompiledArtifact {
            specifier: self.specifier,
            kind: JsCompiledArtifactKind::Module,
            source: self.source,
            strict: true,
            main,
            code_blocks: self.code_blocks,
            imports,
            exports,
            requests,
            metadata: BTreeMap::new(),
        })
    }

    fn compile_synthetic_host_module(
        specifier: impl Into<String>,
        service: impl Into<String>,
        exports: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<JsCompiledArtifact, JsCompileError> {
        let specifier = specifier.into();
        let service = service.into();
        let mut block = JsCompiledCodeBlock {
            id: JsCodeBlockId::new(1),
            label: format!("<synthetic:{specifier}>"),
            parameters: Vec::new(),
            instructions: vec![JsInstruction::PushUndefined, JsInstruction::Return],
            instruction_spans: vec![JsSourceSpan::default(), JsSourceSpan::default()],
            async_function: false,
            strict: true,
        };
        if block.instructions.is_empty() {
            block.instructions.push(JsInstruction::PushUndefined);
            block.instructions.push(JsInstruction::Return);
            block
                .instruction_spans
                .extend([JsSourceSpan::default(), JsSourceSpan::default()]);
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "synthetic_host_service".to_string(),
            serde_json::Value::String(service),
        );
        Ok(JsCompiledArtifact {
            specifier,
            kind: JsCompiledArtifactKind::SyntheticModule,
            source: String::new(),
            strict: true,
            main: block.id,
            code_blocks: BTreeMap::from([(block.id, block)]),
            imports: Vec::new(),
            exports: exports
                .into_iter()
                .map(|name| {
                    let name = name.into();
                    JsCompiledExport {
                        exported_name: name.clone(),
                        local_name: name,
                    }
                })
                .collect(),
            requests: Vec::new(),
            metadata,
        })
    }

    fn compile_module_imports(
        &self,
        items: &ModuleItemList,
    ) -> Result<Vec<JsCompiledImport>, JsCompileError> {
        let mut imports = Vec::new();
        for item in items.items() {
            if let ModuleItem::ImportDeclaration(import) = item {
                imports.push(self.compile_import_declaration(import)?);
            }
        }
        Ok(imports)
    }

    fn compile_import_declaration(
        &self,
        import: &ImportDeclaration,
    ) -> Result<JsCompiledImport, JsCompileError> {
        let mut named_bindings = Vec::new();
        let mut namespace_binding = None;
        match import.kind() {
            ImportKind::DefaultOrUnnamed => {}
            ImportKind::Namespaced { binding } => {
                namespace_binding = Some(self.identifier(*binding));
            }
            ImportKind::Named { names } => {
                for specifier in names.iter().copied() {
                    named_bindings.push((
                        self.identifier(specifier.binding()),
                        self.symbol(specifier.export_name()),
                    ));
                }
            }
        }
        let attributes = import
            .attributes()
            .iter()
            .map(|attribute| (self.symbol(attribute.key()), self.symbol(attribute.value())))
            .collect::<BTreeMap<_, _>>();
        Ok(JsCompiledImport {
            request: self.symbol(import.specifier().sym()),
            default_binding: import.default().map(|id| self.identifier(id)),
            namespace_binding,
            named_bindings,
            attributes,
        })
    }

    fn compile_module_exports(
        &self,
        items: &ModuleItemList,
    ) -> Result<Vec<JsCompiledExport>, JsCompileError> {
        let mut exports = Vec::new();
        for item in items.items() {
            let ModuleItem::ExportDeclaration(export) = item else {
                continue;
            };
            match export.as_ref() {
                ExportDeclaration::DefaultAssignmentExpression(_) => {
                    exports.push(JsCompiledExport {
                        exported_name: "default".to_string(),
                        local_name: "__terrace_export_default".to_string(),
                    });
                }
                ExportDeclaration::VarStatement(var) => {
                    for variable in var.0.as_ref() {
                        exports.push(self.export_for_variable(variable)?);
                    }
                }
                ExportDeclaration::Declaration(declaration) => {
                    exports.extend(self.exports_for_declaration(declaration)?);
                }
                _ => {
                    return Err(JsCompileError::UnsupportedSyntax {
                        specifier: self.specifier.clone(),
                        detail: format!("unsupported export declaration {export:?}"),
                    });
                }
            }
        }
        Ok(exports)
    }

    fn compile_module_items(
        &mut self,
        items: &ModuleItemList,
    ) -> Result<JsCodeBlockId, JsCompileError> {
        let mut statements = Vec::new();
        for item in items.items() {
            match item {
                ModuleItem::ImportDeclaration(_) => {}
                ModuleItem::StatementListItem(item) => statements.push(item.clone()),
                ModuleItem::ExportDeclaration(export) => match export.as_ref() {
                    ExportDeclaration::VarStatement(var) => {
                        statements.push(StatementListItem::Statement(Box::new(Statement::Var(
                            var.clone(),
                        ))));
                    }
                    ExportDeclaration::Declaration(declaration) => {
                        statements.push(StatementListItem::Declaration(Box::new(
                            declaration.clone(),
                        )));
                    }
                    ExportDeclaration::DefaultAssignmentExpression(expr) => {
                        statements.push(StatementListItem::Statement(Box::new(
                            Statement::Expression(Expression::Assign(Assign::new(
                                AssignOp::Assign,
                                AssignTarget::Identifier(Identifier::new(
                                    self.intern("__terrace_export_default"),
                                    expr.span(),
                                )),
                                expr.clone(),
                            ))),
                        )));
                    }
                    unsupported => {
                        return Err(JsCompileError::UnsupportedSyntax {
                            specifier: self.specifier.clone(),
                            detail: format!("unsupported module export body {unsupported:?}"),
                        });
                    }
                },
            }
        }
        self.compile_statement_list("<module>", &statements, true, true, None, None)
    }

    fn compile_statement_list(
        &mut self,
        label: impl Into<String>,
        statements: &[StatementListItem],
        strict: bool,
        module_scope: bool,
        parameters: Option<Vec<String>>,
        async_function: Option<bool>,
    ) -> Result<JsCodeBlockId, JsCompileError> {
        let code_block_id = self.allocate_code_block_id();
        let mut builder = JsCodeBlockBuilder::new(
            code_block_id,
            label.into(),
            parameters.unwrap_or_default(),
            async_function.unwrap_or(false),
            strict,
        );
        if module_scope {
            builder.push(
                JsInstruction::DeclareName {
                    name: "__terrace_export_default".to_string(),
                    binding: JsBindingKind::Let,
                },
                JsSourceSpan::default(),
            );
            builder.push(JsInstruction::PushUndefined, JsSourceSpan::default());
            builder.push(
                JsInstruction::StoreName {
                    name: "__terrace_export_default".to_string(),
                },
                JsSourceSpan::default(),
            );
            builder.push(JsInstruction::Pop, JsSourceSpan::default());
        }
        for item in statements {
            self.compile_statement_list_item(item, &mut builder)?;
        }
        builder.push(JsInstruction::PushUndefined, JsSourceSpan::default());
        builder.push(JsInstruction::Return, JsSourceSpan::default());
        self.code_blocks.insert(code_block_id, builder.finish());
        Ok(code_block_id)
    }

    fn compile_statement_list_item(
        &mut self,
        item: &StatementListItem,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        match item {
            StatementListItem::Statement(statement) => self.compile_statement(statement, builder),
            StatementListItem::Declaration(declaration) => {
                self.compile_declaration(declaration, builder)
            }
        }
    }

    fn compile_statement(
        &mut self,
        statement: &Statement,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        match statement {
            Statement::Expression(expression) => {
                self.compile_expression(expression, builder)?;
                builder.push(JsInstruction::Dup, span_for(expression));
                builder.push(
                    JsInstruction::StoreName {
                        name: "__terrace_last_expression".to_string(),
                    },
                    span_for(expression),
                );
                builder.push(JsInstruction::Pop, span_for(expression));
                Ok(())
            }
            Statement::Var(var) => self.compile_variable_list(&var.0, JsBindingKind::Var, builder),
            Statement::Return(ret) => {
                if let Some(target) = ret.target() {
                    self.compile_expression(target, builder)?;
                } else {
                    builder.push(JsInstruction::PushUndefined, span_for(statement));
                }
                builder.push(JsInstruction::Return, span_for(statement));
                Ok(())
            }
            Statement::Block(block) => {
                for item in block.statement_list().statements() {
                    self.compile_statement_list_item(item, builder)?;
                }
                Ok(())
            }
            Statement::Empty => Ok(()),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported statement {unsupported:?}"),
            }),
        }
    }

    fn compile_declaration(
        &mut self,
        declaration: &Declaration,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        match declaration {
            Declaration::Lexical(LexicalDeclaration::Const(list)) => {
                self.compile_variable_list(list, JsBindingKind::Const, builder)
            }
            Declaration::Lexical(LexicalDeclaration::Let(list)) => {
                self.compile_variable_list(list, JsBindingKind::Let, builder)
            }
            Declaration::FunctionDeclaration(function) => self.compile_named_function(
                function.name(),
                function.parameters(),
                function.body(),
                false,
                builder,
                span_for(function),
            ),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported declaration {unsupported:?}"),
            }),
        }
    }

    fn compile_named_function(
        &mut self,
        name: Identifier,
        parameters: &boa_ast::function::FormalParameterList,
        body: &FunctionBody,
        async_function: bool,
        builder: &mut JsCodeBlockBuilder,
        span: JsSourceSpan,
    ) -> Result<(), JsCompileError> {
        let function_id =
            self.compile_function_body(self.identifier(name), parameters, body, async_function)?;
        builder.push(
            JsInstruction::DeclareName {
                name: self.identifier(name),
                binding: JsBindingKind::Let,
            },
            span.clone(),
        );
        builder.push(
            JsInstruction::CreateFunction {
                code_block: function_id,
                async_function,
            },
            span.clone(),
        );
        builder.push(
            JsInstruction::StoreName {
                name: self.identifier(name),
            },
            span.clone(),
        );
        builder.push(JsInstruction::Pop, span);
        Ok(())
    }

    fn compile_function_body(
        &mut self,
        label: String,
        parameters: &boa_ast::function::FormalParameterList,
        body: &FunctionBody,
        async_function: bool,
    ) -> Result<JsCodeBlockId, JsCompileError> {
        let parameters = parameters
            .as_ref()
            .iter()
            .map(|parameter| self.parameter_name(parameter))
            .collect::<Result<Vec<_>, _>>()?;
        self.compile_statement_list(
            format!("<function:{label}>"),
            body.statements(),
            body.strict(),
            false,
            Some(parameters),
            Some(async_function),
        )
    }

    fn compile_variable_list(
        &mut self,
        list: &VariableList,
        binding: JsBindingKind,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        for variable in list.as_ref() {
            let name = self.variable_name(variable)?;
            builder.push(
                JsInstruction::DeclareName {
                    name: name.clone(),
                    binding: binding.clone(),
                },
                span_for(variable),
            );
            if let Some(init) = variable.init() {
                self.compile_expression(init, builder)?;
            } else {
                builder.push(JsInstruction::PushUndefined, span_for(variable));
            }
            builder.push(JsInstruction::StoreName { name }, span_for(variable));
            builder.push(JsInstruction::Pop, span_for(variable));
        }
        Ok(())
    }

    fn compile_expression(
        &mut self,
        expression: &Expression,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        match expression {
            Expression::Literal(literal) => self.compile_literal(literal, builder),
            Expression::Identifier(identifier) => {
                builder.push(
                    JsInstruction::LoadName {
                        name: self.identifier(*identifier),
                    },
                    span_for(expression),
                );
                Ok(())
            }
            Expression::ObjectLiteral(object) => self.compile_object_literal(object, builder),
            Expression::ArrayLiteral(array) => {
                for element in array.as_ref() {
                    if let Some(element) = element {
                        self.compile_expression(element, builder)?;
                    } else {
                        builder.push(JsInstruction::PushUndefined, span_for(expression));
                    }
                }
                builder.push(
                    JsInstruction::CreateArray {
                        element_count: array.as_ref().len(),
                    },
                    span_for(expression),
                );
                Ok(())
            }
            Expression::PropertyAccess(access) => self.compile_property_access(access, builder),
            Expression::Call(call) => self.compile_call(call, builder),
            Expression::Await(await_expr) => {
                self.compile_expression(await_expr.target(), builder)?;
                builder.push(JsInstruction::Await, span_for(expression));
                Ok(())
            }
            Expression::Assign(assign) => self.compile_assign(assign, builder),
            Expression::ArrowFunction(function) => {
                self.compile_arrow_function(function, false, builder, span_for(expression))
            }
            Expression::AsyncArrowFunction(function) => {
                self.compile_async_arrow_function(function, builder, span_for(expression))
            }
            Expression::FunctionExpression(function) => {
                self.compile_function_expression(function, false, builder, span_for(expression))
            }
            Expression::Unary(unary) => self.compile_unary(unary, builder),
            Expression::Binary(binary) => self.compile_binary(binary, builder),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported expression {unsupported:?}"),
            }),
        }
    }

    fn compile_literal(
        &self,
        literal: &Literal,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        let instruction = match literal.kind() {
            LiteralKind::String(sym) => JsInstruction::PushString {
                value: self.symbol(*sym),
            },
            LiteralKind::Num(value) => JsInstruction::PushNumber { value: *value },
            LiteralKind::Int(value) => JsInstruction::PushNumber {
                value: *value as f64,
            },
            LiteralKind::Bool(value) => JsInstruction::PushBool { value: *value },
            LiteralKind::Null => JsInstruction::PushNull,
            LiteralKind::Undefined => JsInstruction::PushUndefined,
            unsupported => {
                return Err(JsCompileError::UnsupportedSyntax {
                    specifier: self.specifier.clone(),
                    detail: format!("unsupported literal {unsupported:?}"),
                });
            }
        };
        builder.push(instruction, span_for(literal));
        Ok(())
    }

    fn compile_object_literal(
        &mut self,
        object: &boa_ast::expression::literal::ObjectLiteral,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        builder.push(JsInstruction::CreateObject, span_for(object));
        for property in object.properties() {
            match property {
                PropertyDefinition::Property(name, value) => {
                    builder.push(JsInstruction::Dup, span_for(property));
                    self.compile_expression(value, builder)?;
                    builder.push(
                        JsInstruction::DefineProperty {
                            name: self.property_name(name)?,
                        },
                        span_for(property),
                    );
                }
                unsupported => {
                    return Err(JsCompileError::UnsupportedSyntax {
                        specifier: self.specifier.clone(),
                        detail: format!("unsupported object property {unsupported:?}"),
                    });
                }
            }
        }
        Ok(())
    }

    fn compile_property_access(
        &mut self,
        access: &PropertyAccess,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        let PropertyAccess::Simple(access) = access else {
            return Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported property access {access:?}"),
            });
        };
        self.compile_expression(access.target(), builder)?;
        match access.field() {
            PropertyAccessField::Const(ident) => builder.push(
                JsInstruction::GetProperty {
                    name: self.identifier(*ident),
                },
                span_for(access),
            ),
            PropertyAccessField::Expr(expr) => {
                self.compile_expression(expr, builder)?;
                builder.push(JsInstruction::GetPropertyDynamic, span_for(access));
            }
        }
        Ok(())
    }

    fn compile_call(
        &mut self,
        call: &Call,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        if let Expression::PropertyAccess(PropertyAccess::Simple(access)) = call.function() {
            self.compile_expression(access.target(), builder)?;
            for argument in call.args() {
                self.compile_expression(argument, builder)?;
            }
            match access.field() {
                PropertyAccessField::Const(ident) => builder.push(
                    JsInstruction::CallMethod {
                        name: self.identifier(*ident),
                        argc: call.args().len(),
                    },
                    span_for(call),
                ),
                PropertyAccessField::Expr(expr) => {
                    self.compile_expression(expr, builder)?;
                    builder.push(
                        JsInstruction::CallMethod {
                            name: "__terrace_dynamic".to_string(),
                            argc: call.args().len(),
                        },
                        span_for(call),
                    );
                }
            }
            return Ok(());
        }
        self.compile_expression(call.function(), builder)?;
        for argument in call.args() {
            self.compile_expression(argument, builder)?;
        }
        builder.push(
            JsInstruction::Call {
                argc: call.args().len(),
            },
            span_for(call),
        );
        Ok(())
    }

    fn compile_assign(
        &mut self,
        assign: &Assign,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        if assign.op() != AssignOp::Assign {
            return Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported assignment operator {:?}", assign.op()),
            });
        }
        match assign.lhs() {
            AssignTarget::Identifier(identifier) => {
                self.compile_expression(assign.rhs(), builder)?;
                builder.push(
                    JsInstruction::StoreName {
                        name: self.identifier(*identifier),
                    },
                    span_for(assign),
                );
                Ok(())
            }
            AssignTarget::Access(PropertyAccess::Simple(access)) => {
                self.compile_expression(access.target(), builder)?;
                self.compile_expression(assign.rhs(), builder)?;
                match access.field() {
                    PropertyAccessField::Const(ident) => builder.push(
                        JsInstruction::SetProperty {
                            name: self.identifier(*ident),
                        },
                        span_for(assign),
                    ),
                    PropertyAccessField::Expr(expr) => {
                        self.compile_expression(expr, builder)?;
                        builder.push(JsInstruction::SetPropertyDynamic, span_for(assign));
                    }
                }
                Ok(())
            }
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported assignment target {unsupported:?}"),
            }),
        }
    }

    fn compile_unary(
        &mut self,
        unary: &Unary,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        self.compile_expression(unary.target(), builder)?;
        let op = match unary.op() {
            UnaryOp::Plus => JsUnaryOperator::Plus,
            UnaryOp::Minus => JsUnaryOperator::Minus,
            UnaryOp::Not => JsUnaryOperator::Not,
            UnaryOp::TypeOf => JsUnaryOperator::TypeOf,
            unsupported => {
                return Err(JsCompileError::UnsupportedSyntax {
                    specifier: self.specifier.clone(),
                    detail: format!("unsupported unary operator {unsupported:?}"),
                });
            }
        };
        builder.push(JsInstruction::Unary { op }, span_for(unary));
        Ok(())
    }

    fn compile_binary(
        &mut self,
        binary: &Binary,
        builder: &mut JsCodeBlockBuilder,
    ) -> Result<(), JsCompileError> {
        self.compile_expression(binary.lhs(), builder)?;
        self.compile_expression(binary.rhs(), builder)?;
        let op = match binary.op() {
            BinaryOp::Arithmetic(ArithmeticOp::Add) => JsBinaryOperator::Add,
            BinaryOp::Relational(RelationalOp::StrictEqual) => JsBinaryOperator::StrictEq,
            BinaryOp::Relational(RelationalOp::StrictNotEqual) => JsBinaryOperator::StrictNe,
            BinaryOp::Relational(RelationalOp::Equal) => JsBinaryOperator::Eq,
            BinaryOp::Relational(RelationalOp::NotEqual) => JsBinaryOperator::Ne,
            BinaryOp::Logical(LogicalOp::And) => JsBinaryOperator::LogicalAnd,
            BinaryOp::Logical(LogicalOp::Or) => JsBinaryOperator::LogicalOr,
            BinaryOp::Logical(LogicalOp::Coalesce) => JsBinaryOperator::NullishCoalesce,
            unsupported => {
                return Err(JsCompileError::UnsupportedSyntax {
                    specifier: self.specifier.clone(),
                    detail: format!("unsupported binary operator {unsupported:?}"),
                });
            }
        };
        builder.push(JsInstruction::Binary { op }, span_for(binary));
        Ok(())
    }

    fn compile_arrow_function(
        &mut self,
        function: &ArrowFunction,
        async_function: bool,
        builder: &mut JsCodeBlockBuilder,
        span: JsSourceSpan,
    ) -> Result<(), JsCompileError> {
        let code_block = self.compile_function_body(
            function
                .name()
                .map(|name| self.identifier(name))
                .unwrap_or_else(|| "<arrow>".to_string()),
            function.parameters(),
            function.body(),
            async_function,
        )?;
        builder.push(
            JsInstruction::CreateFunction {
                code_block,
                async_function,
            },
            span,
        );
        Ok(())
    }

    fn compile_async_arrow_function(
        &mut self,
        function: &AsyncArrowFunction,
        builder: &mut JsCodeBlockBuilder,
        span: JsSourceSpan,
    ) -> Result<(), JsCompileError> {
        let code_block = self.compile_function_body(
            function
                .name()
                .map(|name| self.identifier(name))
                .unwrap_or_else(|| "<async-arrow>".to_string()),
            function.parameters(),
            function.body(),
            true,
        )?;
        builder.push(
            JsInstruction::CreateFunction {
                code_block,
                async_function: true,
            },
            span,
        );
        Ok(())
    }

    fn compile_function_expression(
        &mut self,
        function: &FunctionExpression,
        async_function: bool,
        builder: &mut JsCodeBlockBuilder,
        span: JsSourceSpan,
    ) -> Result<(), JsCompileError> {
        let code_block = self.compile_function_body(
            function
                .name()
                .map(|name| self.identifier(name))
                .unwrap_or_else(|| "<function>".to_string()),
            function.parameters(),
            function.body(),
            async_function,
        )?;
        builder.push(
            JsInstruction::CreateFunction {
                code_block,
                async_function,
            },
            span,
        );
        Ok(())
    }

    fn allocate_code_block_id(&mut self) -> JsCodeBlockId {
        let id = JsCodeBlockId::new(self.next_code_block_id);
        self.next_code_block_id += 1;
        id
    }

    fn variable_name(&self, variable: &Variable) -> Result<String, JsCompileError> {
        match variable.binding() {
            Binding::Identifier(identifier) => Ok(self.identifier(*identifier)),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported binding pattern {unsupported:?}"),
            }),
        }
    }

    fn parameter_name(&self, parameter: &FormalParameter) -> Result<String, JsCompileError> {
        if parameter.is_rest_param() {
            return Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: "rest parameters are not yet supported".to_string(),
            });
        }
        if parameter.init().is_some() {
            return Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: "default parameter expressions are not yet supported".to_string(),
            });
        }
        self.variable_name(parameter.variable())
    }

    fn exports_for_declaration(
        &self,
        declaration: &Declaration,
    ) -> Result<Vec<JsCompiledExport>, JsCompileError> {
        match declaration {
            Declaration::FunctionDeclaration(function) => Ok(vec![JsCompiledExport {
                exported_name: self.identifier(function.name()),
                local_name: self.identifier(function.name()),
            }]),
            Declaration::Lexical(LexicalDeclaration::Const(list))
            | Declaration::Lexical(LexicalDeclaration::Let(list)) => list
                .as_ref()
                .iter()
                .map(|variable| self.export_for_variable(variable))
                .collect(),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported declaration export {unsupported:?}"),
            }),
        }
    }

    fn export_for_variable(&self, variable: &Variable) -> Result<JsCompiledExport, JsCompileError> {
        let name = self.variable_name(variable)?;
        Ok(JsCompiledExport {
            exported_name: name.clone(),
            local_name: name,
        })
    }

    fn property_name(&self, name: &PropertyName) -> Result<String, JsCompileError> {
        match name {
            PropertyName::Literal(identifier) => Ok(self.identifier(*identifier)),
            unsupported => Err(JsCompileError::UnsupportedSyntax {
                specifier: self.specifier.clone(),
                detail: format!("unsupported property name {unsupported:?}"),
            }),
        }
    }

    fn identifier(&self, identifier: Identifier) -> String {
        self.symbol(identifier.sym())
    }

    fn symbol(&self, sym: boa_interner::Sym) -> String {
        format!("{}", self.interner.resolve_expect(sym))
    }

    fn intern(&mut self, value: &str) -> boa_interner::Sym {
        self.interner.get_or_intern(value)
    }
}

struct JsCodeBlockBuilder {
    id: JsCodeBlockId,
    label: String,
    parameters: Vec<String>,
    instructions: Vec<JsInstruction>,
    instruction_spans: Vec<JsSourceSpan>,
    async_function: bool,
    strict: bool,
}

impl JsCodeBlockBuilder {
    fn new(
        id: JsCodeBlockId,
        label: String,
        parameters: Vec<String>,
        async_function: bool,
        strict: bool,
    ) -> Self {
        Self {
            id,
            label,
            parameters,
            instructions: Vec::new(),
            instruction_spans: Vec::new(),
            async_function,
            strict,
        }
    }

    fn push(&mut self, instruction: JsInstruction, span: JsSourceSpan) {
        self.instructions.push(instruction);
        self.instruction_spans.push(span);
    }

    fn finish(self) -> JsCompiledCodeBlock {
        JsCompiledCodeBlock {
            id: self.id,
            label: self.label,
            parameters: self.parameters,
            instructions: self.instructions,
            instruction_spans: self.instruction_spans,
            async_function: self.async_function,
            strict: self.strict,
        }
    }
}

fn span_for<T>(_node: &T) -> JsSourceSpan {
    JsSourceSpan::default()
}

pub fn compile_script_artifact(
    specifier: impl Into<String>,
    source: impl Into<String>,
) -> Result<JsCompiledArtifact, JsCompileError> {
    JsArtifactCompiler::new(specifier, source).compile_script()
}

pub fn compile_module_artifact(
    specifier: impl Into<String>,
    source: impl Into<String>,
) -> Result<JsCompiledArtifact, JsCompileError> {
    JsArtifactCompiler::new(specifier, source).compile_module()
}

pub fn compile_synthetic_host_module_artifact(
    specifier: impl Into<String>,
    service: impl Into<String>,
    exports: impl IntoIterator<Item = impl Into<String>>,
) -> Result<JsCompiledArtifact, JsCompileError> {
    JsArtifactCompiler::compile_synthetic_host_module(specifier, service, exports)
}

#[cfg(test)]
mod tests {
    use super::{
        JsBindingKind, JsCompiledArtifactKind, JsInstruction, compile_module_artifact,
        compile_script_artifact, compile_synthetic_host_module_artifact,
    };

    #[test]
    fn script_compilation_produces_immutable_code_block() {
        let artifact = compile_script_artifact(
            "terrace:/script.js",
            r#"
const state = { done: false };
Promise.resolve().then(() => {
  state.done = true;
});
state;
"#,
        )
        .expect("compile script");

        assert_eq!(artifact.kind, JsCompiledArtifactKind::Script);
        let block = artifact
            .code_blocks
            .get(&artifact.main)
            .expect("main block");
        assert_eq!(block.parameters, Vec::<String>::new());
        assert!(block.instructions.iter().any(|instruction| matches!(
            instruction,
            JsInstruction::DeclareName { name, binding: JsBindingKind::Const }
                if name == "state"
        )));
        assert!(block.instructions.iter().any(|instruction| matches!(
            instruction,
            JsInstruction::CallMethod { name, argc: 1 } if name == "then"
        )));
    }

    #[test]
    fn module_compilation_tracks_requests_imports_and_default_export() {
        let artifact = compile_module_artifact(
            "terrace:/main.mjs",
            r#"
import helper from "./helper.mjs";
import { echo } from "terrace:host/echo";

const response = await echo({ message: helper.message });
export default { helper: helper.helper, echoed: response.echoed };
"#,
        )
        .expect("compile module");

        assert_eq!(artifact.kind, JsCompiledArtifactKind::Module);
        assert_eq!(
            artifact.requests,
            vec!["./helper.mjs".to_string(), "terrace:host/echo".to_string()]
        );
        assert_eq!(artifact.imports.len(), 2);
        assert_eq!(artifact.exports.len(), 1);
        assert_eq!(artifact.exports[0].exported_name, "default");
        let block = artifact
            .code_blocks
            .get(&artifact.main)
            .expect("main block");
        assert!(
            block
                .instructions
                .iter()
                .any(|instruction| matches!(instruction, JsInstruction::Await))
        );
    }

    #[test]
    fn synthetic_host_module_compilation_preserves_exports() {
        let artifact = compile_synthetic_host_module_artifact(
            "terrace:host/echo",
            "terrace:host/echo",
            ["echo", "status"],
        )
        .expect("compile synthetic module");
        assert_eq!(artifact.kind, JsCompiledArtifactKind::SyntheticModule);
        assert_eq!(
            artifact
                .exports
                .iter()
                .map(|export| export.exported_name.as_str())
                .collect::<Vec<_>>(),
            vec!["echo", "status"]
        );
    }
}
