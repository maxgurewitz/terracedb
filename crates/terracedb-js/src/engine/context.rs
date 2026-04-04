use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
    thread::ThreadId,
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{
    JsRuntimeConfiguration,
    engine::heap::{
        JsHeapConfig, JsHeapError, JsHeapObjectDescriptor, JsHeapObjectId, JsHeapRootId,
        JsHeapSnapshot, JsRuntimeHeap, JsRuntimeHeapAttachment,
    },
};

macro_rules! context_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
    };
}

context_id!(JsRealmId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsContextServiceConfiguration {
    pub host_hooks: String,
    pub clock: String,
    pub job_executor: String,
    pub module_loader: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl Default for JsContextServiceConfiguration {
    fn default() -> Self {
        Self {
            host_hooks: "terrace-host-hooks".to_string(),
            clock: "terrace-clock".to_string(),
            job_executor: "terrace-job-executor".to_string(),
            module_loader: "terrace-module-loader".to_string(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsRealmSnapshot {
    pub realm_id: JsRealmId,
    pub label: String,
    pub global_object_id: JsHeapObjectId,
    pub global_this_id: JsHeapObjectId,
    pub global_object_root_id: JsHeapRootId,
    pub global_this_root_id: JsHeapRootId,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub host_defined: BTreeMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub loaded_modules: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub registered_classes: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsContextSnapshot {
    pub configuration: JsRuntimeConfiguration,
    pub services: JsContextServiceConfiguration,
    pub current_realm_id: JsRealmId,
    pub realms: BTreeMap<JsRealmId, JsRealmSnapshot>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsContextAttachmentSnapshot {
    pub attached: bool,
    pub attachment_epoch: u64,
    pub current_realm_id: Option<JsRealmId>,
    pub thread_id: Option<String>,
    pub label: Option<String>,
}

impl Default for JsContextAttachmentSnapshot {
    fn default() -> Self {
        Self {
            attached: false,
            attachment_epoch: 0,
            current_realm_id: None,
            thread_id: None,
            label: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsContextDetachedSnapshot {
    pub context: JsContextSnapshot,
    pub attachment: JsContextAttachmentSnapshot,
    pub heap: JsHeapSnapshot,
}

#[derive(Debug, Error)]
pub enum JsContextError {
    #[error("context realm not found: {realm_id:?}")]
    RealmNotFound { realm_id: JsRealmId },
    #[error("context must contain at least one realm")]
    MissingRealm,
    #[error("cannot drop the currently active realm {realm_id:?}")]
    CannotDropCurrentRealm { realm_id: JsRealmId },
    #[error("context is already attached")]
    AlreadyAttached,
    #[error("context is not attached")]
    NotAttached,
    #[error(transparent)]
    Heap(#[from] JsHeapError),
}

#[derive(Clone)]
pub struct JsRuntimeContext {
    heap: JsRuntimeHeap,
    inner: Arc<Mutex<JsRuntimeContextState>>,
}

pub struct JsRuntimeContextBuilder {
    configuration: JsRuntimeConfiguration,
    services: JsContextServiceConfiguration,
    heap: Option<JsRuntimeHeap>,
    heap_config: JsHeapConfig,
    default_realm_label: String,
    metadata: BTreeMap<String, JsonValue>,
}

impl JsRuntimeContextBuilder {
    pub fn new(configuration: JsRuntimeConfiguration) -> Self {
        Self {
            configuration,
            services: JsContextServiceConfiguration::default(),
            heap: None,
            heap_config: JsHeapConfig::default(),
            default_realm_label: "default".to_string(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_services(mut self, services: JsContextServiceConfiguration) -> Self {
        self.services = services;
        self
    }

    pub fn with_heap(mut self, heap: JsRuntimeHeap) -> Self {
        self.heap = Some(heap);
        self
    }

    pub fn with_heap_config(mut self, heap_config: JsHeapConfig) -> Self {
        self.heap_config = heap_config;
        self
    }

    pub fn with_default_realm_label(mut self, label: impl Into<String>) -> Self {
        self.default_realm_label = label.into();
        self
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, JsonValue>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn build(self) -> Result<JsRuntimeContext, JsContextError> {
        let heap = self
            .heap
            .unwrap_or_else(|| JsRuntimeHeap::new(self.heap_config.clone()));
        let context = JsRuntimeContext {
            heap,
            inner: Arc::new(Mutex::new(JsRuntimeContextState {
                next_realm_id: 0,
                snapshot: JsContextSnapshot {
                    configuration: self.configuration,
                    services: self.services,
                    current_realm_id: JsRealmId::default(),
                    realms: BTreeMap::new(),
                    metadata: self.metadata,
                },
                attachment: JsContextAttachmentSnapshot::default(),
            })),
        };

        {
            let mut state = context
                .inner
                .lock()
                .expect("js runtime context mutex poisoned during bootstrap");
            let mut heap = context.heap.attach("context-bootstrap");
            let label = self.default_realm_label;
            let global_object_id = heap.allocate_object(JsHeapObjectDescriptor::labeled(
                "realm_global_object",
                label.clone(),
                128,
            ))?;
            let global_this_id = heap.allocate_object(JsHeapObjectDescriptor::labeled(
                "realm_global_this",
                label.clone(),
                64,
            ))?;
            let global_object_root_id =
                heap.create_root(global_object_id, format!("{label}::global_object"))?;
            let global_this_root_id =
                heap.create_root(global_this_id, format!("{label}::global_this"))?;
            let realm_id = JsRealmId::new(1);
            state.next_realm_id = 1;
            state.snapshot.current_realm_id = realm_id;
            state.snapshot.realms.insert(
                realm_id,
                JsRealmSnapshot {
                    realm_id,
                    label,
                    global_object_id,
                    global_this_id,
                    global_object_root_id,
                    global_this_root_id,
                    host_defined: BTreeMap::new(),
                    loaded_modules: BTreeSet::new(),
                    registered_classes: BTreeSet::new(),
                },
            );
            let _ = heap.detach()?;
        }

        Ok(context)
    }
}

impl JsRuntimeContext {
    pub fn builder(configuration: JsRuntimeConfiguration) -> JsRuntimeContextBuilder {
        JsRuntimeContextBuilder::new(configuration)
    }

    pub fn heap(&self) -> JsRuntimeHeap {
        self.heap.clone()
    }

    pub fn snapshot(&self) -> JsContextDetachedSnapshot {
        let state = self
            .inner
            .lock()
            .expect("js runtime context mutex poisoned during snapshot");
        JsContextDetachedSnapshot {
            context: state.snapshot.clone(),
            attachment: state.attachment.clone(),
            heap: self.heap.snapshot(),
        }
    }

    pub fn attach(
        &self,
        label: impl Into<String>,
    ) -> Result<JsAttachedRuntimeContext<'_>, JsContextError> {
        let mut state = self
            .inner
            .lock()
            .expect("js runtime context mutex poisoned during attach");
        if state.attachment.attached {
            return Err(JsContextError::AlreadyAttached);
        }
        if state.snapshot.realms.is_empty() {
            return Err(JsContextError::MissingRealm);
        }
        state.attachment.attached = true;
        state.attachment.attachment_epoch = state.attachment.attachment_epoch.saturating_add(1);
        state.attachment.current_realm_id = Some(state.snapshot.current_realm_id);
        state.attachment.thread_id = Some(format!("{:?}", std::thread::current().id()));
        state.attachment.label = Some(label.into());
        let heap = self.heap.attach("context-attachment");
        Ok(JsAttachedRuntimeContext {
            state,
            heap,
            owner_thread: std::thread::current().id(),
        })
    }
}

pub struct JsAttachedRuntimeContext<'a> {
    state: MutexGuard<'a, JsRuntimeContextState>,
    heap: JsRuntimeHeapAttachment<'a>,
    owner_thread: ThreadId,
}

impl<'a> JsAttachedRuntimeContext<'a> {
    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }

    pub fn configuration(&self) -> &JsRuntimeConfiguration {
        &self.state.snapshot.configuration
    }

    pub fn services(&self) -> &JsContextServiceConfiguration {
        &self.state.snapshot.services
    }

    pub fn attachment_snapshot(&self) -> JsContextAttachmentSnapshot {
        self.state.attachment.clone()
    }

    pub fn current_realm_id(&self) -> JsRealmId {
        self.state.snapshot.current_realm_id
    }

    pub fn current_realm(&self) -> Result<&JsRealmSnapshot, JsContextError> {
        self.state
            .snapshot
            .realms
            .get(&self.state.snapshot.current_realm_id)
            .ok_or(JsContextError::RealmNotFound {
                realm_id: self.state.snapshot.current_realm_id,
            })
    }

    pub fn realm(&self, realm_id: JsRealmId) -> Result<&JsRealmSnapshot, JsContextError> {
        self.state
            .snapshot
            .realms
            .get(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })
    }

    pub fn switch_realm(&mut self, realm_id: JsRealmId) -> Result<(), JsContextError> {
        if !self.state.snapshot.realms.contains_key(&realm_id) {
            return Err(JsContextError::RealmNotFound { realm_id });
        }
        self.state.snapshot.current_realm_id = realm_id;
        self.state.attachment.current_realm_id = Some(realm_id);
        Ok(())
    }

    pub fn create_realm(&mut self, label: impl Into<String>) -> Result<JsRealmId, JsContextError> {
        let label = label.into();
        let global_object_id = self.heap.allocate_object(JsHeapObjectDescriptor::labeled(
            "realm_global_object",
            label.clone(),
            128,
        ))?;
        let global_this_id = self.heap.allocate_object(JsHeapObjectDescriptor::labeled(
            "realm_global_this",
            label.clone(),
            64,
        ))?;
        let global_object_root_id = self
            .heap
            .create_root(global_object_id, format!("{label}::global_object"))?;
        let global_this_root_id = self
            .heap
            .create_root(global_this_id, format!("{label}::global_this"))?;

        self.state.next_realm_id = self.state.next_realm_id.saturating_add(1);
        let realm_id = JsRealmId::new(self.state.next_realm_id);
        self.state.snapshot.realms.insert(
            realm_id,
            JsRealmSnapshot {
                realm_id,
                label,
                global_object_id,
                global_this_id,
                global_object_root_id,
                global_this_root_id,
                host_defined: BTreeMap::new(),
                loaded_modules: BTreeSet::new(),
                registered_classes: BTreeSet::new(),
            },
        );
        if self.state.snapshot.realms.len() == 1 {
            self.state.snapshot.current_realm_id = realm_id;
            self.state.attachment.current_realm_id = Some(realm_id);
        }
        Ok(realm_id)
    }

    pub fn drop_realm(&mut self, realm_id: JsRealmId) -> Result<(), JsContextError> {
        if self.state.snapshot.current_realm_id == realm_id {
            return Err(JsContextError::CannotDropCurrentRealm { realm_id });
        }
        let realm = self
            .state
            .snapshot
            .realms
            .remove(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        self.heap.drop_root(realm.global_object_root_id)?;
        self.heap.drop_root(realm.global_this_root_id)?;
        Ok(())
    }

    pub fn set_host_defined(
        &mut self,
        realm_id: JsRealmId,
        key: impl Into<String>,
        value: JsonValue,
    ) -> Result<(), JsContextError> {
        let realm = self
            .state
            .snapshot
            .realms
            .get_mut(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        realm.host_defined.insert(key.into(), value);
        Ok(())
    }

    pub fn host_defined(
        &self,
        realm_id: JsRealmId,
        key: &str,
    ) -> Result<Option<&JsonValue>, JsContextError> {
        let realm = self
            .state
            .snapshot
            .realms
            .get(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        Ok(realm.host_defined.get(key))
    }

    pub fn register_loaded_module(
        &mut self,
        realm_id: JsRealmId,
        specifier: impl Into<String>,
    ) -> Result<(), JsContextError> {
        let realm = self
            .state
            .snapshot
            .realms
            .get_mut(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        realm.loaded_modules.insert(specifier.into());
        Ok(())
    }

    pub fn has_loaded_module(
        &self,
        realm_id: JsRealmId,
        specifier: &str,
    ) -> Result<bool, JsContextError> {
        Ok(self.realm(realm_id)?.loaded_modules.contains(specifier))
    }

    pub fn register_class(
        &mut self,
        realm_id: JsRealmId,
        class_name: impl Into<String>,
    ) -> Result<(), JsContextError> {
        let realm = self
            .state
            .snapshot
            .realms
            .get_mut(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        realm.registered_classes.insert(class_name.into());
        Ok(())
    }

    pub fn unregister_class(
        &mut self,
        realm_id: JsRealmId,
        class_name: &str,
    ) -> Result<bool, JsContextError> {
        let realm = self
            .state
            .snapshot
            .realms
            .get_mut(&realm_id)
            .ok_or(JsContextError::RealmNotFound { realm_id })?;
        Ok(realm.registered_classes.remove(class_name))
    }

    pub fn has_class(&self, realm_id: JsRealmId, class_name: &str) -> Result<bool, JsContextError> {
        Ok(self
            .realm(realm_id)?
            .registered_classes
            .contains(class_name))
    }

    pub fn begin_turn(&mut self, label: impl Into<String>) {
        self.heap.begin_turn(label);
    }

    pub fn reach_safepoint(&mut self, label: impl Into<String>) {
        self.heap.reach_safepoint(label);
    }

    pub fn collect_garbage(&mut self) -> Result<crate::JsGcCycleReport, JsContextError> {
        Ok(self.heap.collect_garbage()?)
    }

    pub fn heap_snapshot(&self) -> JsHeapSnapshot {
        self.heap.snapshot()
    }

    pub fn detach(mut self) -> Result<JsContextDetachedSnapshot, JsContextError> {
        self.state.attachment.attached = false;
        self.state.attachment.thread_id = None;
        self.state.attachment.label = None;
        self.state.attachment.current_realm_id = Some(self.state.snapshot.current_realm_id);
        let heap = self.heap.detach()?;
        Ok(JsContextDetachedSnapshot {
            context: self.state.snapshot.clone(),
            attachment: self.state.attachment.clone(),
            heap,
        })
    }
}

#[derive(Clone, Debug)]
struct JsRuntimeContextState {
    next_realm_id: u64,
    snapshot: JsContextSnapshot,
    attachment: JsContextAttachmentSnapshot,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, thread};

    use serde_json::json;

    use crate::{
        JsForkPolicy, JsRuntimeConfiguration, JsRuntimeEnvironment, JsRuntimePolicy,
        JsRuntimeProvenance,
    };

    use super::{
        JsContextError, JsContextServiceConfiguration, JsRuntimeContext, JsRuntimeContextBuilder,
    };

    fn configuration() -> JsRuntimeConfiguration {
        JsRuntimeConfiguration {
            runtime_id: "runtime-ctx".to_string(),
            policy: JsRuntimePolicy::default(),
            provenance: JsRuntimeProvenance {
                backend: "terrace-js".to_string(),
                host_model: "scheduler-owned".to_string(),
                module_root: "/workspace".to_string(),
                volume_id: None,
                snapshot_sequence: None,
                durable_snapshot: false,
                fork_policy: JsForkPolicy::simulation_native_baseline(),
            },
            environment: JsRuntimeEnvironment {
                locale: "en-US".to_string(),
                timezone: "UTC".to_string(),
                cwd: "/workspace".to_string(),
                env: BTreeMap::from([("LANG".to_string(), "en_US.UTF-8".to_string())]),
                argv: vec!["/terrace/js".to_string(), "main.mjs".to_string()],
                exec_argv: vec!["--conditions=terrace".to_string()],
                exec_path: "/terrace/js".to_string(),
                process_id: 42,
                parent_process_id: 7,
                umask: 0o022,
                temp_dir: Some("/workspace/.tmp".to_string()),
                home_dir: Some("/workspace".to_string()),
                platform: "terrace".to_string(),
                architecture: "x86_64".to_string(),
            },
            metadata: BTreeMap::from([("label".to_string(), json!("test-runtime"))]),
        }
    }

    #[test]
    fn context_builder_injects_environment_and_services() {
        let services = JsContextServiceConfiguration {
            host_hooks: "test-hooks".to_string(),
            clock: "test-clock".to_string(),
            job_executor: "test-jobs".to_string(),
            module_loader: "test-loader".to_string(),
            metadata: BTreeMap::from([("service".to_string(), json!("configured"))]),
        };
        let context = JsRuntimeContext::builder(configuration())
            .with_services(services.clone())
            .with_default_realm_label("primary")
            .build()
            .expect("build context");

        let snapshot = context.snapshot();
        assert_eq!(snapshot.context.configuration.environment.cwd, "/workspace");
        assert_eq!(snapshot.context.configuration.environment.locale, "en-US");
        assert_eq!(snapshot.context.services, services);
        assert_eq!(snapshot.context.realms.len(), 1);
        let realm = snapshot
            .context
            .realms
            .get(&snapshot.context.current_realm_id)
            .expect("default realm");
        assert_eq!(realm.label, "primary");
    }

    #[test]
    fn context_can_create_and_switch_realms() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let mut attached = context.attach("switch").expect("attach");
        let first = attached.current_realm_id();
        let second = attached.create_realm("secondary").expect("create second");
        attached.switch_realm(second).expect("switch to second");
        assert_eq!(attached.current_realm_id(), second);
        attached.switch_realm(first).expect("switch back");
        assert_eq!(attached.current_realm_id(), first);
        attached.detach().expect("detach");
    }

    #[test]
    fn realm_host_defined_state_persists_across_detach_and_thread_handoff() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let mut attached = context.attach("main").expect("attach");
        let realm_id = attached.current_realm_id();
        attached
            .set_host_defined(realm_id, "answer", json!(42))
            .expect("set host defined");
        attached.detach().expect("detach");

        let context_for_thread = context.clone();
        let handle = thread::spawn(move || {
            let attached = context_for_thread
                .attach("worker")
                .expect("attach on worker");
            let realm_id = attached.current_realm_id();
            let value = attached
                .host_defined(realm_id, "answer")
                .expect("host defined access")
                .cloned();
            assert_eq!(value, Some(json!(42)));
            attached.detach().expect("detach on worker");
        });
        handle.join().expect("worker join");
    }

    #[test]
    fn realm_drop_releases_global_roots_and_allows_collection() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let default_realm = context.snapshot().context.current_realm_id;
        let mut attached = context.attach("drop-realm").expect("attach");
        let second = attached
            .create_realm("secondary")
            .expect("create second realm");
        let before_drop = attached.heap_snapshot();
        assert_eq!(before_drop.collector.root_count, 4);

        attached.switch_realm(default_realm).expect("switch back");
        attached.drop_realm(second).expect("drop second");
        let report = attached.collect_garbage().expect("collect");
        assert_eq!(report.live_roots, 2);
        assert_eq!(report.live_objects, 2);
        attached.detach().expect("detach");
    }

    #[test]
    fn realm_loaded_modules_and_classes_are_scoped_per_realm() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let mut attached = context.attach("realm-scope").expect("attach");
        let primary = attached.current_realm_id();
        let secondary = attached.create_realm("secondary").expect("secondary");
        attached
            .register_loaded_module(primary, "terrace:/workspace/main.mjs")
            .expect("register module");
        attached
            .register_class(primary, "URLPattern")
            .expect("register class");

        assert!(
            attached
                .has_loaded_module(primary, "terrace:/workspace/main.mjs")
                .expect("module in primary")
        );
        assert!(
            attached
                .has_class(primary, "URLPattern")
                .expect("class in primary")
        );
        assert!(
            !attached
                .has_loaded_module(secondary, "terrace:/workspace/main.mjs")
                .expect("module in secondary")
        );
        assert!(
            !attached
                .has_class(secondary, "URLPattern")
                .expect("class in secondary")
        );
        attached.detach().expect("detach");
    }

    #[test]
    fn current_realm_cannot_be_dropped() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let mut attached = context.attach("cannot-drop-current").expect("attach");
        let realm_id = attached.current_realm_id();
        assert!(matches!(
            attached.drop_realm(realm_id),
            Err(JsContextError::CannotDropCurrentRealm { .. })
        ));
        attached.detach().expect("detach");
    }

    #[test]
    fn context_can_move_between_threads_after_detach() {
        let context = JsRuntimeContextBuilder::new(configuration())
            .build()
            .expect("build context");
        let attached = context.attach("main").expect("attach");
        let main_thread = attached.owner_thread();
        attached.detach().expect("detach");

        let context_for_thread = context.clone();
        let handle = thread::spawn(move || {
            let attached = context_for_thread.attach("worker").expect("attach worker");
            assert_ne!(attached.owner_thread(), main_thread);
            assert_eq!(attached.current_realm().expect("realm").label, "default");
            attached.detach().expect("detach worker");
        });
        handle.join().expect("worker join");
    }
}
