use std::sync::Arc;

use terracedb::{
    Clock, DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_vfs::{InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

use crate::{
    DefaultSandboxStore, SandboxConfig, SandboxError, SandboxServices, SandboxSession, SandboxStore,
};

pub struct SandboxHarness<S> {
    volumes: Arc<S>,
    store: DefaultSandboxStore<S>,
}

impl<S> SandboxHarness<S>
where
    S: VolumeStore + Send + Sync + 'static,
{
    pub fn new(volumes: Arc<S>, clock: Arc<dyn Clock>, services: SandboxServices) -> Self {
        let store = DefaultSandboxStore::new(volumes.clone(), clock, services);
        Self { volumes, store }
    }

    pub fn volumes(&self) -> Arc<S> {
        self.volumes.clone()
    }

    pub fn store(&self) -> &DefaultSandboxStore<S> {
        &self.store
    }

    pub async fn ensure_volume(&self, volume_id: VolumeId) -> Result<(), SandboxError> {
        self.ensure_volume_with_chunk_size(volume_id, 4096).await
    }

    pub async fn ensure_volume_with_chunk_size(
        &self,
        volume_id: VolumeId,
        chunk_size: u32,
    ) -> Result<(), SandboxError> {
        self.volumes
            .open_volume(
                VolumeConfig::new(volume_id)
                    .with_chunk_size(chunk_size)
                    .with_create_if_missing(true),
            )
            .await?;
        Ok(())
    }

    pub async fn open_session(
        &self,
        base_volume_id: VolumeId,
        session_volume_id: VolumeId,
    ) -> Result<SandboxSession, SandboxError> {
        self.open_session_with(base_volume_id, session_volume_id, |config| config)
            .await
    }

    pub async fn open_session_with<F>(
        &self,
        base_volume_id: VolumeId,
        session_volume_id: VolumeId,
        configure: F,
    ) -> Result<SandboxSession, SandboxError>
    where
        F: FnOnce(SandboxConfig) -> SandboxConfig,
    {
        self.ensure_volume(base_volume_id).await?;
        let config =
            configure(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096));
        self.store.open_session(config).await
    }
}

impl SandboxHarness<InMemoryVfsStore> {
    pub fn deterministic(now: u64, seed: u64, services: SandboxServices) -> Self {
        let dependencies = DbDependencies::new(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
            Arc::new(StubClock::new(Timestamp::new(now))),
            Arc::new(StubRng::seeded(seed)),
        );
        let clock = dependencies.clock.clone();
        let volumes = Arc::new(InMemoryVfsStore::with_dependencies(dependencies));
        Self::new(volumes, clock, services)
    }
}
