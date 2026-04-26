use std::collections::HashMap;

use crate::Error;

use super::attachment::{AttachmentHostCtx, HostModuleInstallCtx};
use super::modules::CompiledModule;
use super::vm::{InstructionBudget, RunResult};
use super::{
    AttachmentInstallCtx, BindingKind, ExportName, GcPolicy, HeapStats, ImportName, JsAttachment,
    JsRuntimeId, JsValue, ModuleId, ModuleKey, ModuleNamespace, ModuleRegistry, ModuleState,
    ObjectId, ResolvedExport, Symbol, SymbolTable, Vm, modules::format_export_name,
};
use super::{BytecodeProgram, Instr, compile_module_source, compile_source_to_bytecode};

const JS_RUNTIME_SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JsRuntimeConfig {
    pub gc_policy: GcPolicy,
}

impl Default for JsRuntimeConfig {
    fn default() -> Self {
        Self {
            gc_policy: GcPolicy::default(),
        }
    }
}

pub struct JsRuntimeInstance {
    id: JsRuntimeId,
    symbols: SymbolTable,
    vm: Vm,
    modules: ModuleRegistry,
}

#[derive(serde::Deserialize)]
struct JsRuntimeSnapshot {
    version: u32,
    runtime_id: JsRuntimeId,
    symbols: SymbolTable,
    vm: Vm,
    modules: ModuleRegistry,
}

#[derive(serde::Serialize)]
struct JsRuntimeSnapshotRef<'a> {
    version: u32,
    runtime_id: JsRuntimeId,
    symbols: &'a SymbolTable,
    vm: &'a Vm,
    modules: &'a ModuleRegistry,
}

impl JsRuntimeInstance {
    pub(crate) fn with_config(id: JsRuntimeId, config: JsRuntimeConfig) -> Self {
        Self {
            id,
            symbols: SymbolTable::new(),
            vm: Vm::with_gc_policy(config.gc_policy),
            modules: ModuleRegistry::new(),
        }
    }

    pub fn id(&self) -> JsRuntimeId {
        self.id
    }

    pub(crate) fn install_attachment(
        &mut self,
        attachment: &dyn JsAttachment,
    ) -> Result<(), Error> {
        let (heap, global_env, host) = self.vm.install_parts_mut();
        let mut ctx = AttachmentInstallCtx {
            runtime_id: self.id,
            symbols: &mut self.symbols,
            heap,
            global_env,
            host,
        };

        attachment.install(&mut ctx)
    }

    pub(crate) fn bind_attachment_host(
        &mut self,
        attachment: &dyn JsAttachment,
    ) -> Result<(), Error> {
        let mut ctx = AttachmentHostCtx {
            runtime_id: self.id,
            host: self.vm.host_mut(),
        };

        attachment.bind_host(&mut ctx)
    }

    pub fn eval(&mut self, source: &str) -> Result<JsValue, Error> {
        let program = compile_source_to_bytecode(source, &mut self.symbols)?;
        let result = self.vm.run(program, &self.symbols);
        let gc_result = self.vm.maybe_run_gc();

        gc_result?;
        result
    }

    pub(crate) fn eval_start(&mut self, source: &str) -> RunResult {
        match compile_source_to_bytecode(source, &mut self.symbols) {
            Ok(program) => match self.vm.start(program) {
                Ok(result) => result,
                Err(err) => RunResult::Failed(err),
            },
            Err(err) => RunResult::Failed(err),
        }
    }

    pub(crate) fn run_for_budget(&mut self, budget: InstructionBudget) -> RunResult {
        let result = self.vm.run_for_budget(budget, &self.symbols);
        if !matches!(result, RunResult::Suspended) {
            if let Err(err) = self.vm.maybe_run_gc() {
                return RunResult::Failed(err);
            }
        }

        result
    }

    pub(crate) fn run_until_complete(&mut self) -> RunResult {
        let result = self.vm.run_until_complete(&self.symbols);
        if !matches!(result, RunResult::Suspended) {
            if let Err(err) = self.vm.maybe_run_gc() {
                return RunResult::Failed(err);
            }
        }

        result
    }

    pub fn heap_stats(&self) -> HeapStats {
        self.vm.heap_stats()
    }

    pub fn force_gc(&mut self) -> Result<usize, Error> {
        self.vm.force_gc()
    }

    pub fn evaluate_module(
        &mut self,
        root: ModuleKey,
        sources: &HashMap<ModuleKey, String>,
        attachments: &[Box<dyn JsAttachment + Send>],
    ) -> Result<ModuleId, Error> {
        let module = self.load_module_graph(root, sources, attachments)?;

        self.link_module(module)?;
        self.evaluate_loaded_module(module)?;
        self.vm.maybe_run_gc()?;

        Ok(module)
    }

    pub fn get_module_export(
        &mut self,
        key: &ModuleKey,
        export_name: ExportName,
    ) -> Result<JsValue, Error> {
        let module = self
            .modules
            .id_for_key(key)
            .ok_or_else(|| Error::JsModuleNotDefined {
                specifier: key.0.clone(),
            })?;

        match self.modules.resolve_export(module, export_name)? {
            ResolvedExport::Binding { binding, .. } => {
                let symbol = self.symbol_for_export_name(export_name);
                self.vm.load_cell(binding, symbol, &self.symbols)
            }
            ResolvedExport::Ambiguous => Err(Error::JsModuleExportAmbiguous {
                export: format_export_name(export_name, &self.symbols),
            }),
            ResolvedExport::Namespace { .. } | ResolvedExport::NotFound => {
                Err(Error::JsModuleExportNotFound {
                    specifier: key.0.clone(),
                    export: format_export_name(export_name, &self.symbols),
                })
            }
        }
    }

    pub(crate) fn module_export_name(&mut self, name: &str) -> ExportName {
        if name == "default" {
            ExportName::Default
        } else {
            ExportName::Named(self.symbols.intern(name))
        }
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>, Error> {
        bincode::serialize(&JsRuntimeSnapshotRef {
            version: JS_RUNTIME_SNAPSHOT_VERSION,
            runtime_id: self.id,
            symbols: &self.symbols,
            vm: &self.vm,
            modules: &self.modules,
        })
        .map_err(|err| Error::JsSnapshot(err.to_string()))
    }

    pub(crate) fn deserialize(
        bytes: &[u8],
        attachments: &[Box<dyn JsAttachment + Send>],
    ) -> Result<Self, Error> {
        let snapshot = bincode::deserialize::<JsRuntimeSnapshot>(bytes)
            .map_err(|err| Error::JsSnapshot(err.to_string()))?;

        if snapshot.version != JS_RUNTIME_SNAPSHOT_VERSION {
            return Err(Error::JsSnapshot(format!(
                "unsupported snapshot version {}",
                snapshot.version
            )));
        }

        let mut runtime = Self {
            id: snapshot.runtime_id,
            symbols: snapshot.symbols,
            vm: snapshot.vm,
            modules: snapshot.modules,
        };

        for attachment in attachments {
            runtime.bind_attachment_host(attachment.as_ref())?;
        }

        Ok(runtime)
    }

    pub fn destroy(mut self) -> Result<(), Error> {
        self.vm.destroy()
    }

    fn load_module_graph(
        &mut self,
        key: ModuleKey,
        sources: &HashMap<ModuleKey, String>,
        attachments: &[Box<dyn JsAttachment + Send>],
    ) -> Result<ModuleId, Error> {
        if let Some(id) = self.modules.id_for_key(&key) {
            return Ok(id);
        }

        let Some(source) = sources.get(&key) else {
            return self.load_host_module(key, attachments);
        };
        let compiled = compile_module_source(source, &mut self.symbols)?;
        let env_frame = self.vm.create_module_frame();
        let id = self.modules.insert_loaded(key, env_frame, compiled);
        let requested = self.modules.get(id)?.requested_modules.clone();

        for dependency in requested {
            self.load_module_graph(dependency, sources, attachments)?;
        }

        Ok(id)
    }

    fn load_host_module(
        &mut self,
        key: ModuleKey,
        attachments: &[Box<dyn JsAttachment + Send>],
    ) -> Result<ModuleId, Error> {
        let Some(attachment) = attachments
            .iter()
            .find(|attachment| attachment.has_module(&key))
        else {
            return Err(Error::JsModuleNotDefined {
                specifier: key.0.clone(),
            });
        };

        let env_frame = self.vm.create_module_frame();
        let local_export_entries = {
            let (heap, env, _) = self.vm.install_parts_mut();
            let mut ctx =
                HostModuleInstallCtx::new(self.id, &mut self.symbols, heap, env, env_frame);

            if !attachment.install_module(&key, &mut ctx)? {
                return Err(Error::JsModuleNotDefined {
                    specifier: key.0.clone(),
                });
            }

            ctx.into_local_exports()
        };
        let mut program = BytecodeProgram::new();
        program.instructions.push(Instr::Halt);

        Ok(self.modules.insert_loaded(
            key,
            env_frame,
            CompiledModule {
                requested_modules: Vec::new(),
                import_entries: Vec::new(),
                local_export_entries,
                indirect_export_entries: Vec::new(),
                star_export_entries: Vec::new(),
                local_bindings: Vec::new(),
                program,
            },
        ))
    }

    fn link_module(&mut self, module: ModuleId) -> Result<(), Error> {
        match self.modules.get(module)?.state {
            ModuleState::Linked | ModuleState::Evaluating | ModuleState::Evaluated => {
                return Ok(());
            }
            ModuleState::Linking => return Ok(()),
            ModuleState::Failed => {
                return Err(Error::JsModuleNotDefined {
                    specifier: self.modules.get(module)?.key.0.clone(),
                });
            }
            ModuleState::New | ModuleState::Loading | ModuleState::Loaded => {}
        }

        self.modules.get_mut(module)?.state = ModuleState::Linking;

        self.create_module_bindings(module)?;

        let requested = self.modules.get(module)?.requested_modules.clone();
        for dependency in requested {
            let dependency = self.modules.get_by_key(&dependency)?.id;
            self.link_module(dependency)?;
        }

        self.resolve_module_imports(module)?;
        self.modules.get_mut(module)?.state = ModuleState::Linked;

        Ok(())
    }

    fn create_module_bindings(&mut self, module: ModuleId) -> Result<(), Error> {
        let (env_frame, local_bindings, local_exports) = {
            let record = self.modules.get(module)?;
            (
                record.env_frame,
                record.local_bindings.clone(),
                record.local_export_entries.clone(),
            )
        };

        for binding in local_bindings {
            if self
                .vm
                .cell_for_name_in_frame(env_frame, binding.name, &self.symbols)
                .is_err()
            {
                self.vm
                    .declare_in_frame(env_frame, binding.name, binding.kind, &self.symbols)?;
            }
        }

        let mut export_cells = HashMap::new();

        for export in local_exports {
            let cell =
                self.vm
                    .cell_for_name_in_frame(env_frame, export.local_name, &self.symbols)?;
            export_cells.insert(export.export_name, cell);
        }

        self.modules.get_mut(module)?.export_cells = export_cells;

        Ok(())
    }

    fn resolve_module_imports(&mut self, module: ModuleId) -> Result<(), Error> {
        let (env_frame, imports) = {
            let record = self.modules.get(module)?;
            (record.env_frame, record.import_entries.clone())
        };

        for import in imports {
            let dependency = self.modules.get_by_key(&import.module_request)?.id;

            match import.import_name {
                ImportName::Named(name) => {
                    let cell = self.resolve_required_binding(
                        dependency,
                        ExportName::Named(name),
                        &import.module_request,
                    )?;
                    self.vm
                        .alias_in_frame(env_frame, import.local_name, cell, &self.symbols)?;
                }
                ImportName::Default => {
                    let cell = self.resolve_required_binding(
                        dependency,
                        ExportName::Default,
                        &import.module_request,
                    )?;
                    self.vm
                        .alias_in_frame(env_frame, import.local_name, cell, &self.symbols)?;
                }
                ImportName::Namespace => {
                    let object = self.create_module_namespace_object(dependency)?;
                    self.vm.declare_in_frame_value(
                        env_frame,
                        import.local_name,
                        BindingKind::Const,
                        JsValue::Object(object),
                        &self.symbols,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn resolve_required_binding(
        &self,
        module: ModuleId,
        export_name: ExportName,
        request: &ModuleKey,
    ) -> Result<super::BindingCellId, Error> {
        match self.modules.resolve_export(module, export_name)? {
            ResolvedExport::Binding { binding, .. } => Ok(binding),
            ResolvedExport::Ambiguous => Err(Error::JsModuleExportAmbiguous {
                export: format_export_name(export_name, &self.symbols),
            }),
            ResolvedExport::Namespace { .. } | ResolvedExport::NotFound => {
                Err(Error::JsModuleExportNotFound {
                    specifier: request.0.clone(),
                    export: format_export_name(export_name, &self.symbols),
                })
            }
        }
    }

    fn create_module_namespace_object(&mut self, module: ModuleId) -> Result<ObjectId, Error> {
        let mut exports = HashMap::new();

        for export_name in self.modules.exported_names(module)? {
            let ExportName::Named(symbol) = export_name else {
                continue;
            };

            if let ResolvedExport::Binding { binding, .. } =
                self.modules.resolve_export(module, export_name)?
            {
                exports.insert(symbol, binding);
            }
        }

        Ok(self
            .vm
            .alloc_module_namespace(ModuleNamespace { module, exports }))
    }

    fn evaluate_loaded_module(&mut self, module: ModuleId) -> Result<(), Error> {
        match self.modules.get(module)?.state {
            ModuleState::Evaluated => return Ok(()),
            ModuleState::Evaluating => return Ok(()),
            ModuleState::Failed => {
                return Err(Error::JsModuleNotDefined {
                    specifier: self.modules.get(module)?.key.0.clone(),
                });
            }
            _ => {}
        }

        self.modules.get_mut(module)?.state = ModuleState::Evaluating;

        let dependencies = self.modules.get(module)?.requested_modules.clone();
        for dependency in dependencies {
            let dependency = self.modules.get_by_key(&dependency)?.id;
            self.evaluate_loaded_module(dependency)?;
        }

        let (program, env_frame) = {
            let record = self.modules.get(module)?;
            (record.program.clone(), record.env_frame)
        };

        match self.vm.run_in_frame(program, env_frame, &self.symbols) {
            Ok(_) => {
                self.modules.get_mut(module)?.state = ModuleState::Evaluated;
                Ok(())
            }
            Err(err) => {
                self.modules.get_mut(module)?.state = ModuleState::Failed;
                Err(err)
            }
        }
    }

    fn symbol_for_export_name(&mut self, export_name: ExportName) -> Symbol {
        match export_name {
            ExportName::Named(symbol) => symbol,
            ExportName::Default => self.symbols.intern("default"),
        }
    }
}

impl Drop for JsRuntimeInstance {
    fn drop(&mut self) {
        let _ = self.vm.destroy();
    }
}
