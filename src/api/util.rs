use super::*;

pub(super) fn pending_work_sort_key(work: &PendingWork) -> (u8, &str, Option<u32>) {
    let priority = match work.work_type {
        PendingWorkType::Flush => 0,
        PendingWorkType::CurrentStateRetention => 1,
        PendingWorkType::Compaction => 2,
        PendingWorkType::Backup => 3,
        PendingWorkType::Offload => 4,
        PendingWorkType::Prefetch => 5,
    };
    (priority, work.table.as_str(), work.level)
}

pub(super) fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

pub(super) fn mutex_try_lock<T>(mutex: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    mutex.try_lock()
}

pub(super) async fn read_all_file(
    dependencies: &DbDependencies,
    handle: &crate::FileHandle,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    let mut offset = 0;

    loop {
        let chunk = dependencies
            .file_system
            .read_at(handle, offset, CATALOG_READ_CHUNK_LEN)
            .await?;
        if chunk.is_empty() {
            break;
        }

        offset += chunk.len() as u64;
        bytes.extend_from_slice(&chunk);
        if chunk.len() < CATALOG_READ_CHUNK_LEN {
            break;
        }
    }

    Ok(bytes)
}

pub(super) async fn read_path(
    dependencies: &DbDependencies,
    path: &str,
) -> Result<Vec<u8>, StorageError> {
    read_source(dependencies, &StorageSource::local_file(path)).await
}

pub(super) async fn read_source(
    dependencies: &DbDependencies,
    source: &StorageSource,
) -> Result<Vec<u8>, StorageError> {
    UnifiedStorage::from_dependencies(dependencies)
        .read_all(source)
        .await
        .map_err(|error| error.into_storage_error())
}

pub(super) async fn read_optional_path(
    dependencies: &DbDependencies,
    path: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    match dependencies
        .file_system
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await
    {
        Ok(handle) => Ok(Some(read_all_file(dependencies, &handle).await?)),
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) async fn read_optional_remote_object(
    dependencies: &DbDependencies,
    key: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    match dependencies.object_store.get(key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) async fn write_local_file_atomic(
    dependencies: &DbDependencies,
    path: &str,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let temp_path = format!("{path}.tmp");
    let handle = dependencies
        .file_system
        .open(
            &temp_path,
            OpenOptions {
                create: true,
                read: true,
                write: true,
                truncate: true,
                append: false,
            },
        )
        .await?;
    dependencies.file_system.write_at(&handle, 0, bytes).await?;
    dependencies.file_system.sync(&handle).await?;
    dependencies.file_system.rename(&temp_path, path).await?;
    if let Some(parent) = PathBuf::from(path).parent()
        && !parent.as_os_str().is_empty()
    {
        dependencies
            .file_system
            .sync_dir(parent.to_string_lossy().as_ref())
            .await?;
    }
    Ok(())
}

pub(super) async fn delete_local_file_if_exists(
    dependencies: &DbDependencies,
    path: &str,
) -> Result<(), StorageError> {
    match dependencies.file_system.delete(path).await {
        Ok(()) => {
            if let Some(parent) = PathBuf::from(path).parent()
                && !parent.as_os_str().is_empty()
            {
                dependencies
                    .file_system
                    .sync_dir(parent.to_string_lossy().as_ref())
                    .await?;
            }
            Ok(())
        }
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn bloom_hash_pair(key: &[u8]) -> (u64, u64) {
    fn hash_with_seed(key: &[u8], seed: u64) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^ (hash >> 32)
    }

    let first = hash_with_seed(key, 0x9e37_79b9_7f4a_7c15);
    let second = hash_with_seed(key, 0xc2b2_ae3d_27d4_eb4f) | 1;
    (first, second)
}
