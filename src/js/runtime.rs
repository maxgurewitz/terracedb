use crate::Error;

use super::attachment::AttachmentHostCtx;
use super::compile_source_to_bytecode;
use super::vm::{InstructionBudget, RunResult};
use super::{
    AttachmentInstallCtx, GcPolicy, HeapStats, JsAttachment, JsRuntimeId, JsValue, SymbolTable, Vm,
};

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
}

#[derive(serde::Deserialize)]
struct JsRuntimeSnapshot {
    version: u32,
    runtime_id: JsRuntimeId,
    symbols: SymbolTable,
    vm: Vm,
}

#[derive(serde::Serialize)]
struct JsRuntimeSnapshotRef<'a> {
    version: u32,
    runtime_id: JsRuntimeId,
    symbols: &'a SymbolTable,
    vm: &'a Vm,
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

    pub(crate) fn serialize(&self) -> Result<Vec<u8>, Error> {
        bincode::serialize(&JsRuntimeSnapshotRef {
            version: JS_RUNTIME_SNAPSHOT_VERSION,
            runtime_id: self.id,
            symbols: &self.symbols,
            vm: &self.vm,
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
        };

        for attachment in attachments {
            runtime.bind_attachment_host(attachment.as_ref())?;
        }

        Ok(runtime)
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
