use std::{collections::BTreeMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use terracedb_capabilities::{
    CapabilityManifest as PolicyCapabilityManifest, DatabaseCapabilityFamily, ManifestBinding,
};
use terracedb_vfs::JsonValue;

use crate::{HOST_CAPABILITY_PREFIX, SandboxError, SandboxSession};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapability {
    pub name: String,
    pub specifier: String,
    pub description: Option<String>,
    pub typescript_declarations: Option<String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl SandboxCapability {
    pub fn host_module(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            specifier: format!("{HOST_CAPABILITY_PREFIX}{name}"),
            name,
            description: None,
            typescript_declarations: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_typescript_declarations(mut self, declarations: impl Into<String>) -> Self {
        self.typescript_declarations = Some(declarations.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn method0<Output>(&self, name: impl Into<String>) -> CapabilityMethod0<Output> {
        CapabilityMethod0::new(self.specifier.clone(), name.into())
    }

    pub fn method1<Input, Output>(
        &self,
        name: impl Into<String>,
    ) -> CapabilityMethod1<Input, Output> {
        CapabilityMethod1::new(self.specifier.clone(), name.into())
    }

    pub fn validate(&self) -> Result<(), SandboxError> {
        if !self.specifier.starts_with(HOST_CAPABILITY_PREFIX) {
            return Err(SandboxError::InvalidCapabilitySpecifier {
                specifier: self.specifier.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxCapabilityMethod {
    pub name: String,
    pub description: Option<String>,
}

impl SandboxCapabilityMethod {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapabilityModule {
    pub capability: SandboxCapability,
    pub methods: Vec<SandboxCapabilityMethod>,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCallRequest {
    pub specifier: String,
    pub method: String,
    pub args: Vec<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCallResult {
    pub specifier: String,
    pub method: String,
    pub value: JsonValue,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityMethod0<Output> {
    specifier: String,
    name: String,
    output: PhantomData<fn() -> Output>,
}

impl<Output> CapabilityMethod0<Output> {
    fn new(specifier: String, name: String) -> Self {
        Self {
            specifier,
            name,
            output: PhantomData,
        }
    }

    pub fn specifier(&self) -> &str {
        &self.specifier
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<Output> CapabilityMethod0<Output>
where
    Output: DeserializeOwned,
{
    pub async fn invoke(&self, session: &SandboxSession) -> Result<Output, SandboxError> {
        let result = session
            .invoke_capability(CapabilityCallRequest {
                specifier: self.specifier.clone(),
                method: self.name.clone(),
                args: Vec::new(),
            })
            .await?;
        serde_json::from_value(result.value).map_err(Into::into)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityMethod1<Input, Output> {
    specifier: String,
    name: String,
    marker: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output> CapabilityMethod1<Input, Output> {
    fn new(specifier: String, name: String) -> Self {
        Self {
            specifier,
            name,
            marker: PhantomData,
        }
    }

    pub fn specifier(&self) -> &str {
        &self.specifier
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<Input, Output> CapabilityMethod1<Input, Output>
where
    Input: Serialize,
    Output: DeserializeOwned,
{
    pub async fn invoke(
        &self,
        session: &SandboxSession,
        input: &Input,
    ) -> Result<Output, SandboxError> {
        let result = session
            .invoke_capability(CapabilityCallRequest {
                specifier: self.specifier.clone(),
                method: self.name.clone(),
                args: vec![serde_json::to_value(input)?],
            })
            .await?;
        serde_json::from_value(result.value).map_err(Into::into)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub capabilities: Vec<SandboxCapability>,
}

impl CapabilityManifest {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<(), SandboxError> {
        for capability in &self.capabilities {
            capability.validate()?;
        }
        Ok(())
    }

    pub fn get(&self, specifier: &str) -> Option<&SandboxCapability> {
        self.capabilities
            .iter()
            .find(|capability| capability.specifier == specifier)
    }

    pub fn contains(&self, specifier: &str) -> bool {
        self.get(specifier).is_some()
    }
}

#[async_trait]
pub trait CapabilityRegistry: Send + Sync {
    fn manifest(&self) -> CapabilityManifest;
    fn resolve(&self, specifier: &str) -> Option<SandboxCapability>;

    fn module(&self, _specifier: &str) -> Option<SandboxCapabilityModule> {
        None
    }

    async fn invoke(
        &self,
        _session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        Err(SandboxError::CapabilityUnavailable {
            specifier: request.specifier,
        })
    }
}

type BoxCapabilityFuture =
    Pin<Box<dyn Future<Output = Result<JsonValue, SandboxError>> + Send + 'static>>;
type ErasedCapabilityHandler<State> =
    dyn Fn(State, SandboxSession, Vec<JsonValue>) -> BoxCapabilityFuture + Send + Sync + 'static;

#[derive(Clone)]
struct RegisteredCapabilityMethod<State> {
    description: Option<String>,
    handler: Arc<ErasedCapabilityHandler<State>>,
}

#[derive(Clone)]
pub struct TypedCapabilityModule<State> {
    capability: SandboxCapability,
    methods: BTreeMap<String, RegisteredCapabilityMethod<State>>,
    metadata: BTreeMap<String, JsonValue>,
}

pub struct TypedCapabilityModuleBuilder<State> {
    capability: SandboxCapability,
    methods: BTreeMap<String, RegisteredCapabilityMethod<State>>,
    metadata: BTreeMap<String, JsonValue>,
}

impl<State> TypedCapabilityModuleBuilder<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(capability: SandboxCapability) -> Result<Self, SandboxError> {
        capability.validate()?;
        Ok(Self {
            capability,
            methods: BTreeMap::new(),
            metadata: BTreeMap::new(),
        })
    }

    pub fn with_module_metadata(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn with_method0<Output, D, F, Fut>(
        mut self,
        method: &CapabilityMethod0<Output>,
        description: Option<D>,
        handler: F,
    ) -> Result<Self, SandboxError>
    where
        Output: Serialize + Send + 'static,
        D: Into<String>,
        F: Fn(State, SandboxSession) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Output, SandboxError>> + Send + 'static,
    {
        self.ensure_method_specifier(method.specifier())?;
        let handler = Arc::new(handler);
        let name = method.name().to_string();
        let method_name = name.clone();
        self.methods.insert(
            name,
            RegisteredCapabilityMethod {
                description: description.map(Into::into),
                handler: Arc::new(move |state, session, args| {
                    let handler = handler.clone();
                    let method_name = method_name.clone();
                    Box::pin(async move {
                        if !args.is_empty() {
                            return Err(SandboxError::Service {
                                service: "capabilities",
                                message: format!("{method_name} expects no arguments"),
                            });
                        }
                        let output = handler(state, session).await?;
                        serde_json::to_value(output).map_err(Into::into)
                    })
                }),
            },
        );
        Ok(self)
    }

    pub fn with_method1<Input, Output, D, F, Fut>(
        mut self,
        method: &CapabilityMethod1<Input, Output>,
        description: Option<D>,
        handler: F,
    ) -> Result<Self, SandboxError>
    where
        Input: DeserializeOwned + Send + 'static,
        Output: Serialize + Send + 'static,
        D: Into<String>,
        F: Fn(State, SandboxSession, Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Output, SandboxError>> + Send + 'static,
    {
        self.ensure_method_specifier(method.specifier())?;
        let handler = Arc::new(handler);
        let name = method.name().to_string();
        let method_name = name.clone();
        self.methods.insert(
            name,
            RegisteredCapabilityMethod {
                description: description.map(Into::into),
                handler: Arc::new(move |state, session, args| {
                    let handler = handler.clone();
                    let method_name = method_name.clone();
                    Box::pin(async move {
                        let input = decode_single_argument(&method_name, args)?;
                        let output = handler(state, session, input).await?;
                        serde_json::to_value(output).map_err(Into::into)
                    })
                }),
            },
        );
        Ok(self)
    }

    pub fn build(self) -> TypedCapabilityModule<State> {
        TypedCapabilityModule {
            capability: self.capability,
            methods: self.methods,
            metadata: self.metadata,
        }
    }

    fn ensure_method_specifier(&self, specifier: &str) -> Result<(), SandboxError> {
        if specifier == self.capability.specifier {
            Ok(())
        } else {
            Err(SandboxError::Service {
                service: "capabilities",
                message: format!(
                    "capability method specifier {specifier} does not match {}",
                    self.capability.specifier
                ),
            })
        }
    }
}

#[derive(Clone)]
pub struct TypedCapabilityRegistry<State> {
    state: State,
    modules: BTreeMap<String, TypedCapabilityModule<State>>,
}

impl<State> TypedCapabilityRegistry<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(
        state: State,
        modules: Vec<TypedCapabilityModule<State>>,
    ) -> Result<Self, SandboxError> {
        let mut indexed = BTreeMap::new();
        for module in modules {
            module.capability.validate()?;
            if indexed
                .insert(module.capability.specifier.clone(), module)
                .is_some()
            {
                return Err(SandboxError::Service {
                    service: "capabilities",
                    message: "duplicate capability specifier".to_string(),
                });
            }
        }
        Ok(Self {
            state,
            modules: indexed,
        })
    }
}

#[async_trait]
impl<State> CapabilityRegistry for TypedCapabilityRegistry<State>
where
    State: Clone + Send + Sync + 'static,
{
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: self
                .modules
                .values()
                .map(|module| module.capability.clone())
                .collect(),
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.modules
            .get(specifier)
            .map(|module| module.capability.clone())
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        self.modules
            .get(specifier)
            .map(|module| SandboxCapabilityModule {
                capability: module.capability.clone(),
                methods: module
                    .methods
                    .iter()
                    .map(|(name, method)| SandboxCapabilityMethod {
                        name: name.clone(),
                        description: method.description.clone(),
                    })
                    .collect(),
                metadata: module.metadata.clone(),
            })
    }

    async fn invoke(
        &self,
        session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        let module = self.modules.get(&request.specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: request.specifier.clone(),
            }
        })?;
        let method = module.methods.get(&request.method).ok_or_else(|| {
            SandboxError::CapabilityMethodNotFound {
                specifier: request.specifier.clone(),
                method: request.method.clone(),
            }
        })?;
        let value = (method.handler)(self.state.clone(), session.clone(), request.args).await?;
        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value,
            metadata: module.capability.metadata.clone(),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct StaticCapabilityRegistry {
    manifest: CapabilityManifest,
}

impl StaticCapabilityRegistry {
    pub fn new(manifest: CapabilityManifest) -> Result<Self, SandboxError> {
        manifest.validate()?;
        Ok(Self { manifest })
    }
}

#[async_trait]
impl CapabilityRegistry for StaticCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        self.manifest.clone()
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.manifest.get(specifier).cloned()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestBoundCapabilityInvocation {
    pub binding_name: String,
    pub capability_family: String,
    pub method: String,
    pub args: Vec<JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ManifestBoundCapabilityResult {
    pub value: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait ManifestBoundCapabilityDispatcher: Send + Sync {
    async fn invoke_binding(
        &self,
        session: &SandboxSession,
        binding: &ManifestBinding,
        request: ManifestBoundCapabilityInvocation,
    ) -> Result<ManifestBoundCapabilityResult, SandboxError>;
}

#[derive(Clone)]
pub struct ManifestBoundCapabilityRegistry {
    policy_manifest: PolicyCapabilityManifest,
    bindings: BTreeMap<String, ManifestBinding>,
    modules: BTreeMap<String, SandboxCapabilityModule>,
    dispatcher: Arc<dyn ManifestBoundCapabilityDispatcher>,
}

impl ManifestBoundCapabilityRegistry {
    pub fn new(
        policy_manifest: PolicyCapabilityManifest,
        dispatcher: Arc<dyn ManifestBoundCapabilityDispatcher>,
    ) -> Result<Self, SandboxError> {
        let mut bindings = BTreeMap::new();
        let mut modules = BTreeMap::new();

        for binding in &policy_manifest.bindings {
            let family =
                DatabaseCapabilityFamily::parse(&binding.capability_family).ok_or_else(|| {
                    SandboxError::Service {
                        service: "capabilities",
                        message: format!(
                            "unsupported generated capability family {} for binding {}",
                            binding.capability_family, binding.binding_name
                        ),
                    }
                })?;
            let capability = generated_capability_for_binding(binding, &policy_manifest, family)?;
            let module = SandboxCapabilityModule {
                capability,
                methods: family
                    .generated_methods()
                    .iter()
                    .map(|method| SandboxCapabilityMethod::new(*method))
                    .collect(),
                metadata: generated_binding_metadata(binding, &policy_manifest),
            };
            bindings.insert(binding.module_specifier.clone(), binding.clone());
            modules.insert(binding.module_specifier.clone(), module);
        }

        Ok(Self {
            policy_manifest,
            bindings,
            modules,
            dispatcher,
        })
    }

    pub fn policy_manifest(&self) -> &PolicyCapabilityManifest {
        &self.policy_manifest
    }
}

#[async_trait]
impl CapabilityRegistry for ManifestBoundCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: self
                .modules
                .values()
                .map(|module| module.capability.clone())
                .collect(),
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.modules
            .get(specifier)
            .map(|module| module.capability.clone())
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        self.modules.get(specifier).cloned()
    }

    async fn invoke(
        &self,
        session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        let binding = self.bindings.get(&request.specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: request.specifier.clone(),
            }
        })?;
        let module = self.modules.get(&request.specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: request.specifier.clone(),
            }
        })?;
        if !module
            .methods
            .iter()
            .any(|method| method.name == request.method)
        {
            return Err(SandboxError::CapabilityMethodNotFound {
                specifier: request.specifier,
                method: request.method,
            });
        }

        let binding_result = self
            .dispatcher
            .invoke_binding(
                session,
                binding,
                ManifestBoundCapabilityInvocation {
                    binding_name: binding.binding_name.clone(),
                    capability_family: binding.capability_family.clone(),
                    method: request.method.clone(),
                    args: request.args,
                },
            )
            .await?;
        let mut metadata = module.capability.metadata.clone();
        metadata.extend(binding_result.metadata);

        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value: binding_result.value,
            metadata,
        })
    }
}

fn generated_capability_for_binding(
    binding: &ManifestBinding,
    manifest: &PolicyCapabilityManifest,
    family: DatabaseCapabilityFamily,
) -> Result<SandboxCapability, SandboxError> {
    let capability = SandboxCapability {
        name: binding.binding_name.clone(),
        specifier: binding.module_specifier.clone(),
        description: Some(format!(
            "Generated host binding for {} ({})",
            binding.binding_name, binding.capability_family
        )),
        typescript_declarations: Some(
            family.generated_typescript_declarations(&binding.module_specifier),
        ),
        metadata: generated_binding_metadata(binding, manifest),
    };
    capability.validate()?;
    Ok(capability)
}

fn generated_binding_metadata(
    binding: &ManifestBinding,
    manifest: &PolicyCapabilityManifest,
) -> BTreeMap<String, JsonValue> {
    let mut metadata = binding.metadata.clone();
    metadata.insert(
        "binding_name".to_string(),
        JsonValue::String(binding.binding_name.clone()),
    );
    metadata.insert(
        "capability_family".to_string(),
        JsonValue::String(binding.capability_family.clone()),
    );
    metadata.insert(
        "source_template_id".to_string(),
        JsonValue::String(binding.source_template_id.clone()),
    );
    if let Some(source_grant_id) = binding.source_grant_id.as_ref() {
        metadata.insert(
            "source_grant_id".to_string(),
            JsonValue::String(source_grant_id.clone()),
        );
    }
    if let Some(preset_name) = manifest.preset_name.as_ref() {
        metadata.insert(
            "preset_name".to_string(),
            JsonValue::String(preset_name.clone()),
        );
    }
    if let Some(profile_name) = manifest.profile_name.as_ref() {
        metadata.insert(
            "profile_name".to_string(),
            JsonValue::String(profile_name.clone()),
        );
    }
    metadata
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicCapabilityMethodBehavior {
    EchoArgs,
    Static { value: JsonValue },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeterministicCapabilityModule {
    pub capability: SandboxCapability,
    pub methods: BTreeMap<String, DeterministicCapabilityMethodBehavior>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl DeterministicCapabilityModule {
    pub fn new(capability: SandboxCapability) -> Result<Self, SandboxError> {
        capability.validate()?;
        Ok(Self {
            capability,
            methods: BTreeMap::new(),
            metadata: BTreeMap::new(),
        })
    }

    pub fn with_echo_method(mut self, method: impl Into<String>) -> Self {
        self.methods.insert(
            method.into(),
            DeterministicCapabilityMethodBehavior::EchoArgs,
        );
        self
    }

    pub fn with_static_method(mut self, method: impl Into<String>, value: JsonValue) -> Self {
        self.methods.insert(
            method.into(),
            DeterministicCapabilityMethodBehavior::Static { value },
        );
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicCapabilityRegistry {
    modules: BTreeMap<String, DeterministicCapabilityModule>,
}

impl DeterministicCapabilityRegistry {
    pub fn new(modules: Vec<DeterministicCapabilityModule>) -> Result<Self, SandboxError> {
        let mut indexed = BTreeMap::new();
        for module in modules {
            module.capability.validate()?;
            indexed.insert(module.capability.specifier.clone(), module);
        }
        Ok(Self { modules: indexed })
    }
}

fn decode_single_argument<Input>(
    method_name: &str,
    mut args: Vec<JsonValue>,
) -> Result<Input, SandboxError>
where
    Input: DeserializeOwned,
{
    if args.len() != 1 {
        return Err(SandboxError::Service {
            service: "capabilities",
            message: format!("{method_name} expects exactly one argument"),
        });
    }
    serde_json::from_value(args.remove(0)).map_err(Into::into)
}

#[async_trait]
impl CapabilityRegistry for DeterministicCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: self
                .modules
                .values()
                .map(|module| module.capability.clone())
                .collect(),
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.modules
            .get(specifier)
            .map(|module| module.capability.clone())
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        self.modules
            .get(specifier)
            .map(|module| SandboxCapabilityModule {
                capability: module.capability.clone(),
                methods: module
                    .methods
                    .keys()
                    .cloned()
                    .map(SandboxCapabilityMethod::new)
                    .collect(),
                metadata: module.metadata.clone(),
            })
    }

    async fn invoke(
        &self,
        _session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        let module = self.modules.get(&request.specifier).ok_or_else(|| {
            SandboxError::CapabilityUnavailable {
                specifier: request.specifier.clone(),
            }
        })?;
        let behavior = module.methods.get(&request.method).ok_or_else(|| {
            SandboxError::CapabilityMethodNotFound {
                specifier: request.specifier.clone(),
                method: request.method.clone(),
            }
        })?;
        let value = match behavior {
            DeterministicCapabilityMethodBehavior::EchoArgs => json!({
                "specifier": request.specifier,
                "method": request.method,
                "args": request.args,
            }),
            DeterministicCapabilityMethodBehavior::Static { value } => value.clone(),
        };
        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value,
            metadata: module.metadata.clone(),
        })
    }
}
