use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use bytes::Bytes;

use crate::Error;

use super::{
    JsHostIo, JsRuntimeAttachment, JsRuntimeId, JsValue, RuntimeConsole, SymbolTable, Vm,
    compile_source_to_bytecode,
};

pub struct JsRuntimeInstance {
    id: JsRuntimeId,
    symbols: SymbolTable,
    vm: Vm,
    attachments: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl JsRuntimeInstance {
    pub(crate) fn new(id: JsRuntimeId) -> Self {
        Self {
            id,
            symbols: SymbolTable::new(),
            vm: Vm::new(),
            attachments: HashMap::new(),
        }
    }

    pub fn id(&self) -> JsRuntimeId {
        self.id
    }

    pub fn install(&mut self, attachment: Box<dyn JsRuntimeAttachment>) -> Result<(), Error> {
        attachment.install(self)
    }

    pub fn insert_attachment<T>(&mut self, attachment: T) -> Option<T>
    where
        T: Send + 'static,
    {
        let previous = self
            .attachments
            .insert(TypeId::of::<T>(), Box::new(attachment));

        previous.and_then(|attachment| attachment.downcast::<T>().ok().map(|value| *value))
    }

    pub fn attachment<T>(&self) -> Option<&T>
    where
        T: Send + 'static,
    {
        self.attachments
            .get(&TypeId::of::<T>())
            .and_then(|attachment| attachment.downcast_ref::<T>())
    }

    pub fn attachment_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Send + 'static,
    {
        self.attachments
            .get_mut(&TypeId::of::<T>())
            .and_then(|attachment| attachment.downcast_mut::<T>())
    }

    pub fn eval(&mut self, source: &str) -> Result<JsValue, Error> {
        let program = compile_source_to_bytecode(source, &mut self.symbols)?;
        let console = self.attachment::<RuntimeConsole>().cloned();
        let mut host = RuntimeHostIo { console };

        self.vm.run(&program, &self.symbols, &mut host)
    }
}

struct RuntimeHostIo {
    console: Option<RuntimeConsole>,
}

impl JsHostIo for RuntimeHostIo {
    fn console_log(&mut self, value: JsValue) -> Result<(), Error> {
        let console = self.console.as_ref().ok_or(Error::MissingConsole)?;
        let mut line = value.stringify();
        line.push('\n');

        console.stdout.write(Bytes::from(line))
    }
}
