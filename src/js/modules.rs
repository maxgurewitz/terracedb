use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{BindingCellId, BytecodeProgram, EnvFrameId, ModuleId, Symbol, SymbolTable};

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ModuleKey(pub String);

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum ModuleState {
    New,
    Loading,
    Loaded,
    Linking,
    Linked,
    Evaluating,
    Evaluated,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModuleRegistry {
    next_module_id: u64,
    by_key: HashMap<ModuleKey, ModuleId>,
    modules: HashMap<ModuleId, ModuleRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModuleRecord {
    pub id: ModuleId,
    pub key: ModuleKey,
    pub state: ModuleState,
    pub requested_modules: Vec<ModuleKey>,
    pub import_entries: Vec<ImportEntry>,
    pub local_export_entries: Vec<LocalExportEntry>,
    pub indirect_export_entries: Vec<IndirectExportEntry>,
    pub star_export_entries: Vec<StarExportEntry>,
    pub local_bindings: Vec<LocalBindingEntry>,
    pub env_frame: EnvFrameId,
    pub program: BytecodeProgram,
    pub export_cells: HashMap<ExportName, BindingCellId>,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalBindingEntry {
    pub name: Symbol,
    pub kind: super::BindingKind,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImportEntry {
    pub module_request: ModuleKey,
    pub import_name: ImportName,
    pub local_name: Symbol,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ImportName {
    Named(Symbol),
    Default,
    Namespace,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalExportEntry {
    pub export_name: ExportName,
    pub local_name: Symbol,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndirectExportEntry {
    pub export_name: ExportName,
    pub module_request: ModuleKey,
    pub import_name: ExportName,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct StarExportEntry {
    pub module_request: ModuleKey,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ExportName {
    Named(Symbol),
    Default,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResolvedExport {
    Binding {
        module: ModuleId,
        binding: BindingCellId,
    },
    Namespace {
        module: ModuleId,
    },
    Ambiguous,
    NotFound,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self {
            next_module_id: 0,
            by_key: HashMap::new(),
            modules: HashMap::new(),
        }
    }

    pub fn contains_key(&self, key: &ModuleKey) -> bool {
        self.by_key.contains_key(key)
    }

    pub fn id_for_key(&self, key: &ModuleKey) -> Option<ModuleId> {
        self.by_key.get(key).copied()
    }

    pub fn insert_loaded(
        &mut self,
        key: ModuleKey,
        env_frame: EnvFrameId,
        compiled: CompiledModule,
    ) -> ModuleId {
        let id = ModuleId(self.next_module_id);
        self.next_module_id += 1;

        self.by_key.insert(key.clone(), id);
        self.modules.insert(
            id,
            ModuleRecord {
                id,
                key,
                state: ModuleState::Loaded,
                requested_modules: compiled.requested_modules,
                import_entries: compiled.import_entries,
                local_export_entries: compiled.local_export_entries,
                indirect_export_entries: compiled.indirect_export_entries,
                star_export_entries: compiled.star_export_entries,
                local_bindings: compiled.local_bindings,
                env_frame,
                program: compiled.program,
                export_cells: HashMap::new(),
            },
        );

        id
    }

    pub fn get(&self, id: ModuleId) -> Result<&ModuleRecord, Error> {
        self.modules
            .get(&id)
            .ok_or_else(|| Error::JsModuleNotDefined {
                specifier: format!("<module:{}>", id.0),
            })
    }

    pub fn get_mut(&mut self, id: ModuleId) -> Result<&mut ModuleRecord, Error> {
        self.modules
            .get_mut(&id)
            .ok_or_else(|| Error::JsModuleNotDefined {
                specifier: format!("<module:{}>", id.0),
            })
    }

    pub fn get_by_key(&self, key: &ModuleKey) -> Result<&ModuleRecord, Error> {
        let id = self
            .id_for_key(key)
            .ok_or_else(|| Error::JsModuleNotDefined {
                specifier: key.0.clone(),
            })?;

        self.get(id)
    }

    pub fn resolve_export(
        &self,
        module: ModuleId,
        export_name: ExportName,
    ) -> Result<ResolvedExport, Error> {
        let mut visited = HashSet::new();
        self.resolve_export_inner(module, export_name, &mut visited)
    }

    pub fn exported_names(&self, module: ModuleId) -> Result<HashSet<ExportName>, Error> {
        let mut visited = HashSet::new();
        let mut names = HashSet::new();

        self.exported_names_inner(module, &mut visited, &mut names)?;

        Ok(names)
    }

    fn resolve_export_inner(
        &self,
        module: ModuleId,
        export_name: ExportName,
        visited: &mut HashSet<(ModuleId, ExportName)>,
    ) -> Result<ResolvedExport, Error> {
        if !visited.insert((module, export_name)) {
            return Ok(ResolvedExport::NotFound);
        }

        let record = self.get(module)?;

        if let Some(binding) = record.export_cells.get(&export_name).copied() {
            return Ok(ResolvedExport::Binding { module, binding });
        }

        for entry in &record.indirect_export_entries {
            if entry.export_name != export_name {
                continue;
            }

            let target = self.get_by_key(&entry.module_request)?.id;
            return self.resolve_export_inner(target, entry.import_name, visited);
        }

        if export_name == ExportName::Default {
            return Ok(ResolvedExport::NotFound);
        }

        let mut found = None;

        for entry in &record.star_export_entries {
            let target = self.get_by_key(&entry.module_request)?.id;

            match self.resolve_export_inner(target, export_name, visited)? {
                ResolvedExport::Binding { module, binding } => match found {
                    None => found = Some((module, binding)),
                    Some((_, existing)) if existing == binding => {}
                    Some(_) => return Ok(ResolvedExport::Ambiguous),
                },
                ResolvedExport::Ambiguous => return Ok(ResolvedExport::Ambiguous),
                ResolvedExport::Namespace { .. } | ResolvedExport::NotFound => {}
            }
        }

        Ok(found
            .map(|(module, binding)| ResolvedExport::Binding { module, binding })
            .unwrap_or(ResolvedExport::NotFound))
    }

    fn exported_names_inner(
        &self,
        module: ModuleId,
        visited: &mut HashSet<ModuleId>,
        names: &mut HashSet<ExportName>,
    ) -> Result<(), Error> {
        if !visited.insert(module) {
            return Ok(());
        }

        let record = self.get(module)?;

        for entry in &record.local_export_entries {
            names.insert(entry.export_name);
        }

        for entry in &record.indirect_export_entries {
            names.insert(entry.export_name);
        }

        for entry in &record.star_export_entries {
            let target = self.get_by_key(&entry.module_request)?.id;
            let mut star_names = HashSet::new();

            self.exported_names_inner(target, visited, &mut star_names)?;

            for name in star_names {
                if name != ExportName::Default {
                    names.insert(name);
                }
            }
        }

        Ok(())
    }
}

impl Default for ModuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CompiledModule {
    pub requested_modules: Vec<ModuleKey>,
    pub import_entries: Vec<ImportEntry>,
    pub local_export_entries: Vec<LocalExportEntry>,
    pub indirect_export_entries: Vec<IndirectExportEntry>,
    pub star_export_entries: Vec<StarExportEntry>,
    pub local_bindings: Vec<LocalBindingEntry>,
    pub program: BytecodeProgram,
}

pub(crate) fn format_export_name(name: ExportName, symbols: &SymbolTable) -> String {
    match name {
        ExportName::Named(symbol) => symbols.resolve_expect(symbol).to_owned(),
        ExportName::Default => "default".to_owned(),
    }
}
