use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use terracedb::{
    ExecutionDomainBudget, ExecutionDomainOwner, ExecutionDomainPath, ExecutionDomainPlacement,
    ExecutionDomainSpec, ExecutionResourceUsage, ExecutionUsageLease, ResourceManager,
};
use terracedb_capabilities::{ExecutionDomain, ExecutionOperation, ExecutionPolicy};

use crate::{
    BashReport, BashRequest, BashService, PackageInstallReport, PackageInstallRequest,
    PackageInstaller, SandboxError, SandboxExecutionBinding, SandboxExecutionResult,
    SandboxRuntimeBackend, SandboxRuntimeHandle, SandboxRuntimeStateHandle, SandboxServiceBindings,
    SandboxSession, SandboxSessionInfo, TypeCheckReport, TypeCheckRequest, TypeScriptEmitReport,
    TypeScriptService, TypeScriptTranspileReport, TypeScriptTranspileRequest,
};

const SANDBOX_EXECUTION_OPERATIONS: [ExecutionOperation; 4] = [
    ExecutionOperation::DraftSession,
    ExecutionOperation::PackageInstall,
    ExecutionOperation::TypeCheck,
    ExecutionOperation::BashHelper,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxExecutionPlacement {
    path: ExecutionDomainPath,
    owner: ExecutionDomainOwner,
    placement: ExecutionDomainPlacement,
    budget: ExecutionDomainBudget,
    usage_by_operation: BTreeMap<ExecutionOperation, ExecutionResourceUsage>,
    metadata: BTreeMap<String, String>,
}

impl SandboxExecutionPlacement {
    pub fn new(path: ExecutionDomainPath) -> Self {
        let owner = ExecutionDomainOwner::Subsystem {
            database: None,
            name: path.as_string(),
        };
        Self {
            path,
            owner,
            placement: ExecutionDomainPlacement::default(),
            budget: ExecutionDomainBudget::default(),
            usage_by_operation: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_owner(mut self, owner: ExecutionDomainOwner) -> Self {
        self.owner = owner;
        self
    }

    pub fn with_placement(mut self, placement: ExecutionDomainPlacement) -> Self {
        self.placement = placement;
        self
    }

    pub fn with_budget(mut self, budget: ExecutionDomainBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn with_usage(
        mut self,
        operation: ExecutionOperation,
        usage: ExecutionResourceUsage,
    ) -> Self {
        self.usage_by_operation.insert(operation, usage);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    fn usage_for(&self, operation: ExecutionOperation) -> ExecutionResourceUsage {
        self.usage_by_operation
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }

    fn spec(&self) -> ExecutionDomainSpec {
        ExecutionDomainSpec {
            path: self.path.clone(),
            owner: self.owner.clone(),
            budget: self.budget,
            placement: self.placement,
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SandboxExecutionDomainRoute {
    placement: SandboxExecutionPlacement,
    runtime: Option<Arc<dyn SandboxRuntimeBackend>>,
    packages: Option<Arc<dyn PackageInstaller>>,
    typescript: Option<Arc<dyn TypeScriptService>>,
    bash: Option<Arc<dyn BashService>>,
}

impl SandboxExecutionDomainRoute {
    pub fn new(placement: SandboxExecutionPlacement) -> Self {
        Self {
            placement,
            runtime: None,
            packages: None,
            typescript: None,
            bash: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<dyn SandboxRuntimeBackend>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_packages(mut self, packages: Arc<dyn PackageInstaller>) -> Self {
        self.packages = Some(packages);
        self
    }

    pub fn with_typescript(mut self, typescript: Arc<dyn TypeScriptService>) -> Self {
        self.typescript = Some(typescript);
        self
    }

    pub fn with_bash(mut self, bash: Arc<dyn BashService>) -> Self {
        self.bash = Some(bash);
        self
    }
}

#[derive(Clone)]
pub struct SandboxExecutionRouter {
    resource_manager: Arc<dyn ResourceManager>,
    routes: Vec<(ExecutionDomain, SandboxExecutionDomainRoute)>,
}

impl SandboxExecutionRouter {
    pub fn new(resource_manager: Arc<dyn ResourceManager>) -> Self {
        Self {
            resource_manager,
            routes: Vec::new(),
        }
    }

    pub fn with_domain(
        mut self,
        domain: ExecutionDomain,
        route: SandboxExecutionDomainRoute,
    ) -> Self {
        self.resource_manager
            .register_domain(route.placement.spec());
        if let Some(existing) = self
            .routes
            .iter_mut()
            .find(|(candidate, _)| *candidate == domain)
        {
            existing.1 = route;
        } else {
            self.routes.push((domain, route));
        }
        self
    }

    pub(crate) fn bindings_for_policy(
        &self,
        policy: &ExecutionPolicy,
        legacy: &SandboxServiceBindings,
    ) -> Result<SandboxServiceBindings, SandboxError> {
        let mut execution_bindings = BTreeMap::new();
        for operation in SANDBOX_EXECUTION_OPERATIONS {
            execution_bindings.insert(operation, self.binding_for_operation(policy, operation)?);
        }

        Ok(SandboxServiceBindings {
            runtime_backend: execution_bindings
                .get(&ExecutionOperation::DraftSession)
                .map(|binding| binding.backend.clone())
                .unwrap_or_else(|| legacy.runtime_backend.clone()),
            package_installer: execution_bindings
                .get(&ExecutionOperation::PackageInstall)
                .map(|binding| binding.backend.clone())
                .unwrap_or_else(|| legacy.package_installer.clone()),
            git_repository_store: legacy.git_repository_store.clone(),
            pull_request_provider: legacy.pull_request_provider.clone(),
            readonly_view_provider: legacy.readonly_view_provider.clone(),
            typescript_service: execution_bindings
                .get(&ExecutionOperation::TypeCheck)
                .map(|binding| binding.backend.clone())
                .unwrap_or_else(|| legacy.typescript_service.clone()),
            bash_service: execution_bindings
                .get(&ExecutionOperation::BashHelper)
                .map(|binding| binding.backend.clone())
                .unwrap_or_else(|| legacy.bash_service.clone()),
            execution_bindings,
        })
    }

    pub(crate) fn runtime_route(
        &self,
        policy: &ExecutionPolicy,
    ) -> Result<ResolvedExecutionRoute<dyn SandboxRuntimeBackend>, SandboxError> {
        let operation = ExecutionOperation::DraftSession;
        let assignment = policy.assignment_for(operation);
        let route = self.route_for(operation, assignment.domain)?;
        let backend = route
            .runtime
            .clone()
            .ok_or(SandboxError::MissingExecutionBackend {
                operation,
                domain: assignment.domain,
                service: "runtime",
            })?;
        let backend_name = backend.name().to_string();
        Ok(ResolvedExecutionRoute::new(
            backend,
            self.resource_manager.clone(),
            self.binding_from_route(operation, assignment.domain, route, &backend_name, policy)?,
        ))
    }

    pub(crate) fn package_route(
        &self,
        policy: &ExecutionPolicy,
    ) -> Result<ResolvedExecutionRoute<dyn PackageInstaller>, SandboxError> {
        let operation = ExecutionOperation::PackageInstall;
        let assignment = policy.assignment_for(operation);
        let route = self.route_for(operation, assignment.domain)?;
        let backend = route
            .packages
            .clone()
            .ok_or(SandboxError::MissingExecutionBackend {
                operation,
                domain: assignment.domain,
                service: "packages",
            })?;
        let backend_name = backend.name().to_string();
        Ok(ResolvedExecutionRoute::new(
            backend,
            self.resource_manager.clone(),
            self.binding_from_route(operation, assignment.domain, route, &backend_name, policy)?,
        ))
    }

    pub(crate) fn typescript_route(
        &self,
        policy: &ExecutionPolicy,
    ) -> Result<ResolvedExecutionRoute<dyn TypeScriptService>, SandboxError> {
        let operation = ExecutionOperation::TypeCheck;
        let assignment = policy.assignment_for(operation);
        let route = self.route_for(operation, assignment.domain)?;
        let backend = route
            .typescript
            .clone()
            .ok_or(SandboxError::MissingExecutionBackend {
                operation,
                domain: assignment.domain,
                service: "typescript",
            })?;
        let backend_name = backend.name().to_string();
        Ok(ResolvedExecutionRoute::new(
            backend,
            self.resource_manager.clone(),
            self.binding_from_route(operation, assignment.domain, route, &backend_name, policy)?,
        ))
    }

    pub(crate) fn bash_route(
        &self,
        policy: &ExecutionPolicy,
    ) -> Result<ResolvedExecutionRoute<dyn BashService>, SandboxError> {
        let operation = ExecutionOperation::BashHelper;
        let assignment = policy.assignment_for(operation);
        let route = self.route_for(operation, assignment.domain)?;
        let backend = route
            .bash
            .clone()
            .ok_or(SandboxError::MissingExecutionBackend {
                operation,
                domain: assignment.domain,
                service: "bash",
            })?;
        let backend_name = backend.name().to_string();
        Ok(ResolvedExecutionRoute::new(
            backend,
            self.resource_manager.clone(),
            self.binding_from_route(operation, assignment.domain, route, &backend_name, policy)?,
        ))
    }

    fn binding_for_operation(
        &self,
        policy: &ExecutionPolicy,
        operation: ExecutionOperation,
    ) -> Result<SandboxExecutionBinding, SandboxError> {
        let assignment = policy.assignment_for(operation);
        let route = self.route_for(operation, assignment.domain)?;
        let backend = match operation {
            ExecutionOperation::DraftSession => {
                route.runtime.as_ref().map(|backend| backend.name())
            }
            ExecutionOperation::PackageInstall => {
                route.packages.as_ref().map(|backend| backend.name())
            }
            ExecutionOperation::TypeCheck => {
                route.typescript.as_ref().map(|backend| backend.name())
            }
            ExecutionOperation::BashHelper => route.bash.as_ref().map(|backend| backend.name()),
            _ => None,
        }
        .ok_or(SandboxError::MissingExecutionBackend {
            operation,
            domain: assignment.domain,
            service: operation_service_name(operation),
        })?;

        self.binding_from_route(operation, assignment.domain, route, backend, policy)
    }

    fn binding_from_route(
        &self,
        operation: ExecutionOperation,
        domain: ExecutionDomain,
        route: &SandboxExecutionDomainRoute,
        backend: &str,
        policy: &ExecutionPolicy,
    ) -> Result<SandboxExecutionBinding, SandboxError> {
        let assignment = policy.assignment_for(operation);
        let mut metadata = route
            .placement
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), JsonValue::from(value.clone())))
            .collect::<BTreeMap<_, _>>();
        metadata.extend(assignment.metadata.clone());
        Ok(SandboxExecutionBinding {
            domain,
            backend: backend.to_string(),
            domain_path: route.placement.path.clone(),
            placement: route.placement.placement,
            requested_usage: route.placement.usage_for(operation),
            budget: effective_budget(policy, operation),
            placement_tags: assignment.placement_tags.clone(),
            metadata,
        })
    }

    fn route_for(
        &self,
        operation: ExecutionOperation,
        domain: ExecutionDomain,
    ) -> Result<&SandboxExecutionDomainRoute, SandboxError> {
        self.routes
            .iter()
            .find(|(candidate, _)| *candidate == domain)
            .map(|(_, route)| route)
            .ok_or(SandboxError::MissingExecutionRoute { operation, domain })
    }
}

pub(crate) struct ResolvedExecutionRoute<T: ?Sized> {
    backend: Arc<T>,
    resource_manager: Arc<dyn ResourceManager>,
    binding: SandboxExecutionBinding,
}

impl<T: ?Sized> ResolvedExecutionRoute<T> {
    fn new(
        backend: Arc<T>,
        resource_manager: Arc<dyn ResourceManager>,
        binding: SandboxExecutionBinding,
    ) -> Self {
        Self {
            backend,
            resource_manager,
            binding,
        }
    }

    fn acquire(&self, operation: ExecutionOperation) -> Result<ExecutionUsageLease, SandboxError> {
        let lease = ExecutionUsageLease::acquire(
            self.resource_manager.clone(),
            self.binding.domain_path.clone(),
            self.binding.requested_usage,
        );
        if lease.admitted() {
            Ok(lease)
        } else {
            Err(SandboxError::ExecutionDomainOverloaded {
                operation,
                path: self.binding.domain_path.clone(),
                blocked_by: lease.decision().blocked_by.clone(),
            })
        }
    }
}

#[derive(Clone)]
pub(crate) struct RoutedRuntimeBackend {
    inner: Arc<dyn SandboxRuntimeBackend>,
    router: Arc<SandboxExecutionRouter>,
}

impl RoutedRuntimeBackend {
    pub(crate) fn new(
        inner: Arc<dyn SandboxRuntimeBackend>,
        router: Arc<SandboxExecutionRouter>,
    ) -> Self {
        Self { inner, router }
    }
}

#[async_trait(?Send)]
impl SandboxRuntimeBackend for RoutedRuntimeBackend {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn start_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        let Some(policy) = session.provenance.execution_policy.as_ref() else {
            return self.inner.start_session(session).await;
        };
        let route = self.router.runtime_route(policy)?;
        let mut handle = route.backend.start_session(session).await?;
        handle.metadata.extend(execution_metadata(
            policy,
            &route.binding,
            ExecutionOperation::DraftSession,
            None,
        )?);
        Ok(handle)
    }

    async fn resume_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        let Some(policy) = session.provenance.execution_policy.as_ref() else {
            return self.inner.resume_session(session).await;
        };
        let route = self.router.runtime_route(policy)?;
        let mut handle = route.backend.resume_session(session).await?;
        handle.metadata.extend(execution_metadata(
            policy,
            &route.binding,
            ExecutionOperation::DraftSession,
            None,
        )?);
        Ok(handle)
    }

    async fn execute(
        &self,
        session: &SandboxSession,
        handle: &SandboxRuntimeHandle,
        request: crate::SandboxExecutionRequest,
        state: SandboxRuntimeStateHandle,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.execute(session, handle, request, state).await;
        };
        let route = self.router.runtime_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::DraftSession)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::DraftSession,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::DraftSession)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::DraftSession) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::DraftSession)
                    .await;
                return Err(error);
            }
        };
        let mut result = match route.backend.execute(session, handle, request, state).await {
            Ok(result) => result,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::DraftSession)
                    .await;
                return Err(error);
            }
        };
        result.metadata.extend(execution_metadata(
            &policy,
            &route.binding,
            ExecutionOperation::DraftSession,
            Some(reserved_call_count),
        )?);
        Ok(result)
    }

    async fn close_session(
        &self,
        session: &SandboxSessionInfo,
        handle: &SandboxRuntimeHandle,
    ) -> Result<(), SandboxError> {
        let Some(policy) = session.provenance.execution_policy.as_ref() else {
            return self.inner.close_session(session, handle).await;
        };
        let route = self.router.runtime_route(policy)?;
        route.backend.close_session(session, handle).await
    }
}

#[derive(Clone)]
pub(crate) struct RoutedPackageInstaller {
    inner: Arc<dyn PackageInstaller>,
    router: Arc<SandboxExecutionRouter>,
}

impl RoutedPackageInstaller {
    pub(crate) fn new(
        inner: Arc<dyn PackageInstaller>,
        router: Arc<SandboxExecutionRouter>,
    ) -> Self {
        Self { inner, router }
    }
}

#[async_trait]
impl PackageInstaller for RoutedPackageInstaller {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn install(
        &self,
        session: &SandboxSession,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.install(session, request).await;
        };
        let route = self.router.package_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::PackageInstall)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::PackageInstall,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::PackageInstall)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::PackageInstall) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::PackageInstall)
                    .await;
                return Err(error);
            }
        };
        let mut report = match route.backend.install(session, request).await {
            Ok(report) => report,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::PackageInstall)
                    .await;
                return Err(error);
            }
        };
        report.metadata.extend(execution_metadata(
            &policy,
            &route.binding,
            ExecutionOperation::PackageInstall,
            Some(reserved_call_count),
        )?);
        Ok(report)
    }
}

#[derive(Clone)]
pub(crate) struct RoutedTypeScriptService {
    inner: Arc<dyn TypeScriptService>,
    router: Arc<SandboxExecutionRouter>,
}

impl RoutedTypeScriptService {
    pub(crate) fn new(
        inner: Arc<dyn TypeScriptService>,
        router: Arc<SandboxExecutionRouter>,
    ) -> Self {
        Self { inner, router }
    }
}

#[async_trait]
impl TypeScriptService for RoutedTypeScriptService {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn transpile(
        &self,
        session: &SandboxSession,
        request: TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.transpile(session, request).await;
        };
        let route = self.router.typescript_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::TypeCheck)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::TypeCheck,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::TypeCheck)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::TypeCheck) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                return Err(error);
            }
        };
        match route.backend.transpile(session, request).await {
            Ok(report) => Ok(report),
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                Err(error)
            }
        }
    }

    async fn check(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.check(session, request).await;
        };
        let route = self.router.typescript_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::TypeCheck)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::TypeCheck,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::TypeCheck)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::TypeCheck) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                return Err(error);
            }
        };
        match route.backend.check(session, request).await {
            Ok(report) => Ok(report),
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                Err(error)
            }
        }
    }

    async fn emit(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.emit(session, request).await;
        };
        let route = self.router.typescript_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::TypeCheck)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::TypeCheck,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::TypeCheck)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::TypeCheck) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                return Err(error);
            }
        };
        match route.backend.emit(session, request).await {
            Ok(report) => Ok(report),
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::TypeCheck)
                    .await;
                Err(error)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct RoutedBashService {
    inner: Arc<dyn BashService>,
    router: Arc<SandboxExecutionRouter>,
}

impl RoutedBashService {
    pub(crate) fn new(inner: Arc<dyn BashService>, router: Arc<SandboxExecutionRouter>) -> Self {
        Self { inner, router }
    }
}

#[async_trait]
impl BashService for RoutedBashService {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn run(
        &self,
        session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError> {
        let Some(policy) = session.execution_policy().await else {
            return self.inner.run(session, request).await;
        };
        let route = self.router.bash_route(&policy)?;
        let reserved_call_count = session
            .reserve_execution_call_count(ExecutionOperation::BashHelper)
            .await;
        if let Err(error) = enforce_max_calls(
            ExecutionOperation::BashHelper,
            route
                .binding
                .budget
                .as_ref()
                .and_then(|budget| budget.max_calls),
            reserved_call_count,
        ) {
            session
                .release_execution_call_count(ExecutionOperation::BashHelper)
                .await;
            return Err(error);
        }
        let _lease = match route.acquire(ExecutionOperation::BashHelper) {
            Ok(lease) => lease,
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::BashHelper)
                    .await;
                return Err(error);
            }
        };
        match route.backend.run(session, request).await {
            Ok(report) => Ok(report),
            Err(error) => {
                session
                    .release_execution_call_count(ExecutionOperation::BashHelper)
                    .await;
                Err(error)
            }
        }
    }
}

fn execution_metadata(
    policy: &ExecutionPolicy,
    binding: &SandboxExecutionBinding,
    operation: ExecutionOperation,
    call_count: Option<u64>,
) -> Result<BTreeMap<String, JsonValue>, SandboxError> {
    let mut metadata = BTreeMap::from([
        ("operation".to_string(), serde_json::to_value(operation)?),
        (
            "execution_domain".to_string(),
            serde_json::to_value(binding.domain)?,
        ),
        (
            "execution_domain_path".to_string(),
            JsonValue::from(binding.domain_path.as_string()),
        ),
        (
            "execution_placement".to_string(),
            serde_json::to_value(binding.placement)?,
        ),
        (
            "placement_tags".to_string(),
            serde_json::to_value(&binding.placement_tags)?,
        ),
        (
            "execution_requested_usage".to_string(),
            serde_json::to_value(binding.requested_usage)?,
        ),
        (
            "execution_binding".to_string(),
            serde_json::to_value(binding)?,
        ),
        (
            "execution_policy".to_string(),
            serde_json::to_value(policy)?,
        ),
    ]);
    if let Some(call_count) = call_count {
        metadata.insert(
            "execution_call_count".to_string(),
            JsonValue::from(call_count),
        );
    }
    if let Some(budget) = binding.budget.as_ref() {
        metadata.insert(
            "execution_budget".to_string(),
            serde_json::to_value(budget)?,
        );
    }
    Ok(metadata)
}

fn effective_budget(
    policy: &ExecutionPolicy,
    operation: ExecutionOperation,
) -> Option<terracedb_capabilities::BudgetPolicy> {
    policy
        .operations
        .get(&operation)
        .and_then(|assignment| assignment.budget.clone())
        .or_else(|| policy.default_assignment.budget.clone())
}

fn enforce_max_calls(
    operation: ExecutionOperation,
    max_calls: Option<u64>,
    call_count: u64,
) -> Result<(), SandboxError> {
    if let Some(max_calls) = max_calls
        && call_count > max_calls
    {
        return Err(SandboxError::Service {
            service: "execution-policy",
            message: format!("execution policy for {operation:?} exceeded max_calls {max_calls}"),
        });
    }
    Ok(())
}

fn operation_service_name(operation: ExecutionOperation) -> &'static str {
    match operation {
        ExecutionOperation::DraftSession => "runtime",
        ExecutionOperation::PackageInstall => "packages",
        ExecutionOperation::TypeCheck => "typescript",
        ExecutionOperation::BashHelper => "bash",
        ExecutionOperation::MigrationPublication => "migration-publication",
        ExecutionOperation::MigrationApply => "migration-apply",
        ExecutionOperation::ProcedureInvocation => "procedure-invocation",
        ExecutionOperation::McpRequest => "mcp",
    }
}
