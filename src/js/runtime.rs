use crate::Error;

use super::compile_source_to_bytecode;
use super::{
    AttachmentInstallCtx, GcPolicy, HeapStats, JsAttachment, JsRuntimeId, JsValue, SymbolTable, Vm,
};

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
}

impl JsRuntimeInstance {
    pub(crate) fn with_config(id: JsRuntimeId, config: JsRuntimeConfig) -> Self {
        Self {
            id,
            symbols: SymbolTable::new(),
            vm: Vm::with_gc_policy(config.gc_policy),
        }
    }

    pub fn id(&self) -> JsRuntimeId {
        self.id
    }

    pub(crate) fn install_attachment(
        &mut self,
        attachment: &dyn JsAttachment,
    ) -> Result<(), Error> {
        let (heap, global_env) = self.vm.install_parts_mut();
        let mut ctx = AttachmentInstallCtx {
            runtime_id: self.id,
            symbols: &mut self.symbols,
            heap,
            global_env,
        };

        attachment.install(&mut ctx)
    }

    pub fn eval(&mut self, source: &str) -> Result<JsValue, Error> {
        let program = compile_source_to_bytecode(source, &mut self.symbols)?;
        let result = self.vm.run(&program, &self.symbols);
        let gc_result = self.vm.maybe_run_gc();

        gc_result?;
        result
    }

    pub fn heap_stats(&self) -> HeapStats {
        self.vm.heap_stats()
    }

    pub fn force_gc(&mut self) -> Result<usize, Error> {
        self.vm.force_gc()
    }

    pub fn destroy(mut self) -> Result<(), Error> {
        self.vm.destroy()
    }
}

impl Drop for JsRuntimeInstance {
    fn drop(&mut self) {
        let _ = self.vm.destroy();
    }
}
