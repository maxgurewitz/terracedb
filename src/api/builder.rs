use super::*;

use crate::{
    adapters::{LocalDirObjectStore, SystemClock, SystemRng, TokioFileSystem},
    config::SsdConfig,
    io::{Clock, FileSystem, ObjectStore, Rng},
};

pub const DEFAULT_TIERED_MAX_LOCAL_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_S3_PRIMARY_MEM_CACHE_SIZE_BYTES: u64 = 1024 * 1024;

/// User-facing storage settings for opening a Terracedb instance.
///
/// `DbSettings` carries the configuration choices ordinary callers tend to make:
/// storage mode plus the durability/cache knobs attached to that mode. Runtime
/// implementations such as filesystem, object store, clock, RNG, and scheduler
/// live in [`DbComponents`] instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbSettings {
    storage: StorageConfig,
}

impl DbSettings {
    pub fn storage(storage: StorageConfig) -> Self {
        Self { storage }
    }

    pub fn tiered(ssd_path: impl Into<String>, s3: S3Location) -> Self {
        Self::tiered_storage(TieredStorageConfig {
            ssd: SsdConfig {
                path: ssd_path.into(),
            },
            s3,
            max_local_bytes: DEFAULT_TIERED_MAX_LOCAL_BYTES,
            durability: TieredDurabilityMode::GroupCommit,
            local_retention: TieredLocalRetentionMode::Offload,
        })
    }

    pub fn tiered_storage(storage: TieredStorageConfig) -> Self {
        Self::storage(StorageConfig::Tiered(storage))
    }

    pub fn s3_primary(s3: S3Location) -> Self {
        Self::s3_primary_storage(S3PrimaryStorageConfig {
            s3,
            mem_cache_size_bytes: DEFAULT_S3_PRIMARY_MEM_CACHE_SIZE_BYTES,
            auto_flush_interval: None,
        })
    }

    pub fn s3_primary_storage(storage: S3PrimaryStorageConfig) -> Self {
        Self::storage(StorageConfig::S3Primary(storage))
    }

    pub fn storage_config(&self) -> &StorageConfig {
        &self.storage
    }

    pub fn into_storage(self) -> StorageConfig {
        self.storage
    }
}

impl From<StorageConfig> for DbSettings {
    fn from(storage: StorageConfig) -> Self {
        Self::storage(storage)
    }
}

/// Runtime component implementations used when opening a Terracedb instance.
///
/// This keeps advanced embedding hooks explicit while still allowing the
/// builder to provide production defaults for the common case.
#[derive(Clone)]
pub struct DbComponents {
    pub file_system: Arc<dyn FileSystem>,
    pub object_store: Arc<dyn ObjectStore>,
    pub clock: Arc<dyn Clock>,
    pub rng: Arc<dyn Rng>,
    pub scheduler: Option<Arc<dyn Scheduler>>,
}

impl DbComponents {
    pub fn new(
        file_system: Arc<dyn FileSystem>,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn Rng>,
    ) -> Self {
        Self {
            file_system,
            object_store,
            clock,
            rng,
            scheduler: None,
        }
    }

    pub fn production(object_store: Arc<dyn ObjectStore>) -> Self {
        Self::new(
            Arc::new(TokioFileSystem::new()),
            object_store,
            Arc::new(SystemClock),
            Arc::new(SystemRng::default()),
        )
    }

    pub fn production_local(root: impl Into<PathBuf>) -> Self {
        Self::production(Arc::new(LocalDirObjectStore::new(root.into())))
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }
}

impl From<DbDependencies> for DbComponents {
    fn from(dependencies: DbDependencies) -> Self {
        Self::new(
            dependencies.file_system,
            dependencies.object_store,
            dependencies.clock,
            dependencies.rng,
        )
    }
}

impl fmt::Debug for DbComponents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbComponents")
            .field("file_system", &"<dyn FileSystem>")
            .field("object_store", &"<dyn ObjectStore>")
            .field("clock", &"<dyn Clock>")
            .field("rng", &"<dyn Rng>")
            .field(
                "scheduler",
                &self.scheduler.as_ref().map(|_| "<dyn Scheduler>"),
            )
            .finish()
    }
}

/// High-level open API intended for ordinary Terracedb callers.
///
/// It keeps storage settings separate from runtime component injection, while
/// still lowering to the existing [`Db::open`] escape hatch underneath.
///
/// # Examples
///
/// Simple tiered setup with production defaults:
///
/// ```no_run
/// use terracedb::{Db, S3Location};
///
/// # async fn open_db() -> Result<(), terracedb::OpenError> {
/// let db = Db::builder()
///     .tiered(
///         "/var/lib/todo-api/ssd",
///         S3Location {
///             bucket: "todo-api".to_string(),
///             prefix: "prod".to_string(),
///         },
///     )
///     .local_object_store("/var/lib/todo-api/object-store")
///     .open()
///     .await?;
/// # drop(db);
/// # Ok(())
/// # }
/// ```
///
/// Simple s3-primary setup:
///
/// ```no_run
/// use terracedb::{Db, S3Location};
///
/// # async fn open_db() -> Result<(), terracedb::OpenError> {
/// let db = Db::builder()
///     .s3_primary(S3Location {
///         bucket: "analytics".to_string(),
///         prefix: "warehouse".to_string(),
///     })
///     .local_object_store("/var/lib/terracedb/object-store")
///     .open()
///     .await?;
/// # drop(db);
/// # Ok(())
/// # }
/// ```
///
/// Advanced embedding/test setup with explicit components:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use terracedb::{
///     Db, DbComponents, DbSettings, DeterministicRng, NoopScheduler, S3Location, StubClock,
///     StubFileSystem, StubObjectStore,
/// };
///
/// # async fn open_db() -> Result<(), terracedb::OpenError> {
/// let db = Db::builder()
///     .settings(DbSettings::tiered(
///         "/test-db",
///         S3Location {
///             bucket: "terracedb-test".to_string(),
///             prefix: "advanced".to_string(),
///         },
///     ))
///     .components(
///         DbComponents::new(
///             Arc::new(StubFileSystem::default()),
///             Arc::new(StubObjectStore::default()),
///             Arc::new(StubClock::default()),
///             Arc::new(DeterministicRng::seeded(7)),
///         )
///         .with_scheduler(Arc::new(NoopScheduler)),
///     )
///     .open()
///     .await?;
/// # drop(db);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct DbBuilder {
    settings: Option<DbSettings>,
    file_system: Option<Arc<dyn FileSystem>>,
    object_store: Option<Arc<dyn ObjectStore>>,
    clock: Option<Arc<dyn Clock>>,
    rng: Option<Arc<dyn Rng>>,
    scheduler: Option<Arc<dyn Scheduler>>,
}

impl DbBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn settings(mut self, settings: DbSettings) -> Self {
        self.settings = Some(settings);
        self
    }

    pub fn storage(self, storage: StorageConfig) -> Self {
        self.settings(DbSettings::storage(storage))
    }

    pub fn tiered(self, ssd_path: impl Into<String>, s3: S3Location) -> Self {
        self.settings(DbSettings::tiered(ssd_path, s3))
    }

    pub fn tiered_storage(self, storage: TieredStorageConfig) -> Self {
        self.settings(DbSettings::tiered_storage(storage))
    }

    pub fn s3_primary(self, s3: S3Location) -> Self {
        self.settings(DbSettings::s3_primary(s3))
    }

    pub fn s3_primary_storage(self, storage: S3PrimaryStorageConfig) -> Self {
        self.settings(DbSettings::s3_primary_storage(storage))
    }

    pub fn config(mut self, config: DbConfig) -> Self {
        self.settings = Some(DbSettings::from(config.storage));
        self.scheduler = config.scheduler;
        self
    }

    pub fn components(mut self, components: DbComponents) -> Self {
        self.file_system = Some(components.file_system);
        self.object_store = Some(components.object_store);
        self.clock = Some(components.clock);
        self.rng = Some(components.rng);
        self.scheduler = components.scheduler;
        self
    }

    pub fn dependencies(self, dependencies: DbDependencies) -> Self {
        self.components(DbComponents::from(dependencies))
    }

    pub fn file_system(mut self, file_system: Arc<dyn FileSystem>) -> Self {
        self.file_system = Some(file_system);
        self
    }

    pub fn object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    pub fn local_object_store(self, root: impl Into<PathBuf>) -> Self {
        self.object_store(Arc::new(LocalDirObjectStore::new(root.into())))
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn rng(mut self, rng: Arc<dyn Rng>) -> Self {
        self.rng = Some(rng);
        self
    }

    pub fn scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn into_open_parts(self) -> Result<(DbConfig, DbDependencies), OpenError> {
        let settings = self.settings.ok_or_else(|| {
            OpenError::InvalidConfig(
                "db builder requires storage settings; call .tiered(...), .s3_primary(...), \
                 .storage(...), .config(...), or .settings(...)"
                    .to_string(),
            )
        })?;
        let object_store = self.object_store.ok_or_else(|| {
            OpenError::InvalidConfig(
                "db builder requires an object store component; call .local_object_store(...), \
                 .object_store(...), .dependencies(...), or .components(...)"
                    .to_string(),
            )
        })?;
        let dependencies = DbDependencies::new(
            self.file_system
                .unwrap_or_else(|| Arc::new(TokioFileSystem::new())),
            object_store,
            self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            self.rng.unwrap_or_else(|| Arc::new(SystemRng::default())),
        );
        let config = DbConfig {
            storage: settings.into_storage(),
            scheduler: self.scheduler,
        };
        Ok((config, dependencies))
    }

    pub async fn open(self) -> Result<Db, OpenError> {
        let (config, dependencies) = self.into_open_parts()?;
        Db::open(config, dependencies).await
    }
}

impl fmt::Debug for DbBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbBuilder")
            .field("settings", &self.settings)
            .field(
                "file_system",
                &self.file_system.as_ref().map(|_| "<dyn FileSystem>"),
            )
            .field(
                "object_store",
                &self.object_store.as_ref().map(|_| "<dyn ObjectStore>"),
            )
            .field("clock", &self.clock.as_ref().map(|_| "<dyn Clock>"))
            .field("rng", &self.rng.as_ref().map(|_| "<dyn Rng>"))
            .field(
                "scheduler",
                &self.scheduler.as_ref().map(|_| "<dyn Scheduler>"),
            )
            .finish()
    }
}

impl Db {
    /// Returns the higher-level builder API intended for ordinary opens.
    ///
    /// Use [`Db::open`] directly when you already have a fully-assembled
    /// [`DbConfig`] plus [`DbDependencies`] and want the lowest-level escape
    /// hatch.
    pub fn builder() -> DbBuilder {
        DbBuilder::new()
    }
}
