use std::collections::HashSet;

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

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
enum ImportNameKind {
    Named,
    Default,
    Namespace,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
enum ExportNameKind {
    Named,
    Default,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModuleRegistry {
    next_module_id: u64,

    module_live: Vec<bool>,
    module_key: Vec<ModuleKey>,
    module_state: Vec<ModuleState>,
    module_env_frame: Vec<EnvFrameId>,
    module_program: Vec<BytecodeProgram>,
    module_requested_range_start: Vec<u32>,
    module_requested_range_len: Vec<u32>,
    module_import_range_start: Vec<u32>,
    module_import_range_len: Vec<u32>,
    module_local_export_range_start: Vec<u32>,
    module_local_export_range_len: Vec<u32>,
    module_indirect_export_range_start: Vec<u32>,
    module_indirect_export_range_len: Vec<u32>,
    module_star_export_range_start: Vec<u32>,
    module_star_export_range_len: Vec<u32>,
    module_local_binding_range_start: Vec<u32>,
    module_local_binding_range_len: Vec<u32>,
    module_export_cell_range_start: Vec<u32>,
    module_export_cell_range_len: Vec<u32>,

    module_requested_key: Vec<ModuleKey>,

    module_import_request: Vec<ModuleKey>,
    module_import_name_kind: Vec<ImportNameKind>,
    module_import_name_symbol: Vec<Option<Symbol>>,
    module_import_local_name: Vec<Symbol>,

    module_local_export_name_kind: Vec<ExportNameKind>,
    module_local_export_name_symbol: Vec<Option<Symbol>>,
    module_local_export_local_name: Vec<Symbol>,

    module_indirect_export_name_kind: Vec<ExportNameKind>,
    module_indirect_export_name_symbol: Vec<Option<Symbol>>,
    module_indirect_export_request: Vec<ModuleKey>,
    module_indirect_import_name_kind: Vec<ExportNameKind>,
    module_indirect_import_name_symbol: Vec<Option<Symbol>>,

    module_star_export_request: Vec<ModuleKey>,

    module_local_binding_name: Vec<Symbol>,
    module_local_binding_kind: Vec<super::BindingKind>,

    module_export_cell_name_kind: Vec<ExportNameKind>,
    module_export_cell_name_symbol: Vec<Option<Symbol>>,
    module_export_cell: Vec<BindingCellId>,
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
    pub export_cells: Vec<(ExportName, BindingCellId)>,
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
            module_live: Vec::new(),
            module_key: Vec::new(),
            module_state: Vec::new(),
            module_env_frame: Vec::new(),
            module_program: Vec::new(),
            module_requested_range_start: Vec::new(),
            module_requested_range_len: Vec::new(),
            module_import_range_start: Vec::new(),
            module_import_range_len: Vec::new(),
            module_local_export_range_start: Vec::new(),
            module_local_export_range_len: Vec::new(),
            module_indirect_export_range_start: Vec::new(),
            module_indirect_export_range_len: Vec::new(),
            module_star_export_range_start: Vec::new(),
            module_star_export_range_len: Vec::new(),
            module_local_binding_range_start: Vec::new(),
            module_local_binding_range_len: Vec::new(),
            module_export_cell_range_start: Vec::new(),
            module_export_cell_range_len: Vec::new(),
            module_requested_key: Vec::new(),
            module_import_request: Vec::new(),
            module_import_name_kind: Vec::new(),
            module_import_name_symbol: Vec::new(),
            module_import_local_name: Vec::new(),
            module_local_export_name_kind: Vec::new(),
            module_local_export_name_symbol: Vec::new(),
            module_local_export_local_name: Vec::new(),
            module_indirect_export_name_kind: Vec::new(),
            module_indirect_export_name_symbol: Vec::new(),
            module_indirect_export_request: Vec::new(),
            module_indirect_import_name_kind: Vec::new(),
            module_indirect_import_name_symbol: Vec::new(),
            module_star_export_request: Vec::new(),
            module_local_binding_name: Vec::new(),
            module_local_binding_kind: Vec::new(),
            module_export_cell_name_kind: Vec::new(),
            module_export_cell_name_symbol: Vec::new(),
            module_export_cell: Vec::new(),
        }
    }

    pub fn contains_key(&self, key: &ModuleKey) -> bool {
        self.id_for_key(key).is_some()
    }

    pub fn id_for_key(&self, key: &ModuleKey) -> Option<ModuleId> {
        self.module_key
            .iter()
            .enumerate()
            .find(|(index, module_key)| {
                self.module_live.get(*index).copied().unwrap_or(false) && *module_key == key
            })
            .map(|(index, _)| ModuleId(index as u64))
    }

    pub fn insert_loaded(
        &mut self,
        key: ModuleKey,
        env_frame: EnvFrameId,
        compiled: CompiledModule,
    ) -> ModuleId {
        let id = ModuleId(self.next_module_id);
        self.next_module_id += 1;
        let index = id.0 as usize;
        self.ensure_module_slot(index);

        self.module_live[index] = true;
        self.module_key[index] = key;
        self.module_state[index] = ModuleState::Loaded;
        self.module_env_frame[index] = env_frame;
        self.module_program[index] = compiled.program;

        self.module_requested_range_start[index] = self.module_requested_key.len() as u32;
        self.module_requested_range_len[index] = compiled.requested_modules.len() as u32;
        self.module_requested_key.extend(compiled.requested_modules);

        self.module_import_range_start[index] = self.module_import_request.len() as u32;
        self.module_import_range_len[index] = compiled.import_entries.len() as u32;
        for entry in compiled.import_entries {
            self.module_import_request.push(entry.module_request);
            let (kind, symbol) = encode_import_name(entry.import_name);
            self.module_import_name_kind.push(kind);
            self.module_import_name_symbol.push(symbol);
            self.module_import_local_name.push(entry.local_name);
        }

        self.module_local_export_range_start[index] =
            self.module_local_export_local_name.len() as u32;
        self.module_local_export_range_len[index] = compiled.local_export_entries.len() as u32;
        for entry in compiled.local_export_entries {
            let (kind, symbol) = encode_export_name(entry.export_name);
            self.module_local_export_name_kind.push(kind);
            self.module_local_export_name_symbol.push(symbol);
            self.module_local_export_local_name.push(entry.local_name);
        }

        self.module_indirect_export_range_start[index] =
            self.module_indirect_export_request.len() as u32;
        self.module_indirect_export_range_len[index] =
            compiled.indirect_export_entries.len() as u32;
        for entry in compiled.indirect_export_entries {
            let (export_kind, export_symbol) = encode_export_name(entry.export_name);
            let (import_kind, import_symbol) = encode_export_name(entry.import_name);
            self.module_indirect_export_name_kind.push(export_kind);
            self.module_indirect_export_name_symbol.push(export_symbol);
            self.module_indirect_export_request
                .push(entry.module_request);
            self.module_indirect_import_name_kind.push(import_kind);
            self.module_indirect_import_name_symbol.push(import_symbol);
        }

        self.module_star_export_range_start[index] = self.module_star_export_request.len() as u32;
        self.module_star_export_range_len[index] = compiled.star_export_entries.len() as u32;
        self.module_star_export_request.extend(
            compiled
                .star_export_entries
                .into_iter()
                .map(|entry| entry.module_request),
        );

        self.module_local_binding_range_start[index] = self.module_local_binding_name.len() as u32;
        self.module_local_binding_range_len[index] = compiled.local_bindings.len() as u32;
        for entry in compiled.local_bindings {
            self.module_local_binding_name.push(entry.name);
            self.module_local_binding_kind.push(entry.kind);
        }

        self.module_export_cell_range_start[index] = self.module_export_cell.len() as u32;
        self.module_export_cell_range_len[index] = 0;

        id
    }

    pub fn get(&self, id: ModuleId) -> Result<ModuleRecord, Error> {
        let index = self.module_index(id)?;
        Ok(ModuleRecord {
            id,
            key: self.module_key[index].clone(),
            state: self.module_state[index],
            requested_modules: self.requested_modules_for(index),
            import_entries: self.import_entries_for(index),
            local_export_entries: self.local_export_entries_for(index),
            indirect_export_entries: self.indirect_export_entries_for(index),
            star_export_entries: self.star_export_entries_for(index),
            local_bindings: self.local_bindings_for(index),
            env_frame: self.module_env_frame[index],
            program: self.module_program[index].clone(),
            export_cells: self.export_cells_for(index),
        })
    }

    pub fn get_by_key(&self, key: &ModuleKey) -> Result<ModuleRecord, Error> {
        let id = self
            .id_for_key(key)
            .ok_or_else(|| Error::JsModuleNotDefined {
                specifier: key.0.clone(),
            })?;

        self.get(id)
    }

    pub fn set_state(&mut self, id: ModuleId, state: ModuleState) -> Result<(), Error> {
        let index = self.module_index(id)?;
        self.module_state[index] = state;
        Ok(())
    }

    pub fn set_export_cells(
        &mut self,
        id: ModuleId,
        export_cells: Vec<(ExportName, BindingCellId)>,
    ) -> Result<(), Error> {
        let index = self.module_index(id)?;
        self.module_export_cell_range_start[index] = self.module_export_cell.len() as u32;
        self.module_export_cell_range_len[index] = export_cells.len() as u32;

        for (name, cell) in export_cells {
            let (kind, symbol) = encode_export_name(name);
            self.module_export_cell_name_kind.push(kind);
            self.module_export_cell_name_symbol.push(symbol);
            self.module_export_cell.push(cell);
        }

        Ok(())
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

        if let Some((_, binding)) = record
            .export_cells
            .iter()
            .find(|(name, _)| *name == export_name)
            .copied()
        {
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

    fn ensure_module_slot(&mut self, index: usize) {
        if self.module_live.len() <= index {
            self.module_live.resize(index + 1, false);
            self.module_key
                .resize_with(index + 1, || ModuleKey(String::new()));
            self.module_state.resize(index + 1, ModuleState::New);
            self.module_env_frame.resize(
                index + 1,
                EnvFrameId {
                    segment: super::SegmentId(0),
                    slot: 0,
                },
            );
            self.module_program
                .resize_with(index + 1, BytecodeProgram::new);
            self.module_requested_range_start.resize(index + 1, 0);
            self.module_requested_range_len.resize(index + 1, 0);
            self.module_import_range_start.resize(index + 1, 0);
            self.module_import_range_len.resize(index + 1, 0);
            self.module_local_export_range_start.resize(index + 1, 0);
            self.module_local_export_range_len.resize(index + 1, 0);
            self.module_indirect_export_range_start.resize(index + 1, 0);
            self.module_indirect_export_range_len.resize(index + 1, 0);
            self.module_star_export_range_start.resize(index + 1, 0);
            self.module_star_export_range_len.resize(index + 1, 0);
            self.module_local_binding_range_start.resize(index + 1, 0);
            self.module_local_binding_range_len.resize(index + 1, 0);
            self.module_export_cell_range_start.resize(index + 1, 0);
            self.module_export_cell_range_len.resize(index + 1, 0);
        }
    }

    fn module_index(&self, id: ModuleId) -> Result<usize, Error> {
        let index = id.0 as usize;
        if !self.module_live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsModuleNotDefined {
                specifier: format!("<module:{}>", id.0),
            });
        }
        Ok(index)
    }

    fn requested_modules_for(&self, index: usize) -> Vec<ModuleKey> {
        let start = self.module_requested_range_start[index] as usize;
        let len = self.module_requested_range_len[index] as usize;
        self.module_requested_key[start..start + len].to_vec()
    }

    fn import_entries_for(&self, index: usize) -> Vec<ImportEntry> {
        let start = self.module_import_range_start[index] as usize;
        let len = self.module_import_range_len[index] as usize;
        (start..start + len)
            .map(|index| ImportEntry {
                module_request: self.module_import_request[index].clone(),
                import_name: decode_import_name(
                    self.module_import_name_kind[index],
                    self.module_import_name_symbol[index],
                ),
                local_name: self.module_import_local_name[index],
            })
            .collect()
    }

    fn local_export_entries_for(&self, index: usize) -> Vec<LocalExportEntry> {
        let start = self.module_local_export_range_start[index] as usize;
        let len = self.module_local_export_range_len[index] as usize;
        (start..start + len)
            .map(|index| LocalExportEntry {
                export_name: decode_export_name(
                    self.module_local_export_name_kind[index],
                    self.module_local_export_name_symbol[index],
                ),
                local_name: self.module_local_export_local_name[index],
            })
            .collect()
    }

    fn indirect_export_entries_for(&self, index: usize) -> Vec<IndirectExportEntry> {
        let start = self.module_indirect_export_range_start[index] as usize;
        let len = self.module_indirect_export_range_len[index] as usize;
        (start..start + len)
            .map(|index| IndirectExportEntry {
                export_name: decode_export_name(
                    self.module_indirect_export_name_kind[index],
                    self.module_indirect_export_name_symbol[index],
                ),
                module_request: self.module_indirect_export_request[index].clone(),
                import_name: decode_export_name(
                    self.module_indirect_import_name_kind[index],
                    self.module_indirect_import_name_symbol[index],
                ),
            })
            .collect()
    }

    fn star_export_entries_for(&self, index: usize) -> Vec<StarExportEntry> {
        let start = self.module_star_export_range_start[index] as usize;
        let len = self.module_star_export_range_len[index] as usize;
        self.module_star_export_request[start..start + len]
            .iter()
            .cloned()
            .map(|module_request| StarExportEntry { module_request })
            .collect()
    }

    fn local_bindings_for(&self, index: usize) -> Vec<LocalBindingEntry> {
        let start = self.module_local_binding_range_start[index] as usize;
        let len = self.module_local_binding_range_len[index] as usize;
        (start..start + len)
            .map(|index| LocalBindingEntry {
                name: self.module_local_binding_name[index],
                kind: self.module_local_binding_kind[index],
            })
            .collect()
    }

    fn export_cells_for(&self, index: usize) -> Vec<(ExportName, BindingCellId)> {
        let start = self.module_export_cell_range_start[index] as usize;
        let len = self.module_export_cell_range_len[index] as usize;
        (start..start + len)
            .map(|index| {
                (
                    decode_export_name(
                        self.module_export_cell_name_kind[index],
                        self.module_export_cell_name_symbol[index],
                    ),
                    self.module_export_cell[index],
                )
            })
            .collect()
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

fn encode_import_name(name: ImportName) -> (ImportNameKind, Option<Symbol>) {
    match name {
        ImportName::Named(symbol) => (ImportNameKind::Named, Some(symbol)),
        ImportName::Default => (ImportNameKind::Default, None),
        ImportName::Namespace => (ImportNameKind::Namespace, None),
    }
}

fn decode_import_name(kind: ImportNameKind, symbol: Option<Symbol>) -> ImportName {
    match kind {
        ImportNameKind::Named => ImportName::Named(symbol.expect("named import has symbol")),
        ImportNameKind::Default => ImportName::Default,
        ImportNameKind::Namespace => ImportName::Namespace,
    }
}

fn encode_export_name(name: ExportName) -> (ExportNameKind, Option<Symbol>) {
    match name {
        ExportName::Named(symbol) => (ExportNameKind::Named, Some(symbol)),
        ExportName::Default => (ExportNameKind::Default, None),
    }
}

fn decode_export_name(kind: ExportNameKind, symbol: Option<Symbol>) -> ExportName {
    match kind {
        ExportNameKind::Named => ExportName::Named(symbol.expect("named export has symbol")),
        ExportNameKind::Default => ExportName::Default,
    }
}
