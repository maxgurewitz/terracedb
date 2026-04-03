use std::sync::Arc;

use async_trait::async_trait;
use terracedb::LogCursor;

use crate::{
    ActivityOptions, ActivityReceiver, ActivityStream, OverlayVolume, SnapshotOptions, VfsError,
    Volume, VolumeConfig, VolumeExport, VolumeSnapshot, VolumeStore,
};

#[async_trait]
pub trait VfsStoreExt: VolumeStore {
    /// Create a writable overlay directly from a base volume by first capturing
    /// the requested snapshot cut and then opening the overlay against it.
    async fn create_overlay_from_volume(
        &self,
        source: Arc<dyn Volume>,
        snapshot: SnapshotOptions,
        target: VolumeConfig,
    ) -> Result<Arc<dyn OverlayVolume>, VfsError> {
        let base = source.snapshot(snapshot).await?;
        self.create_overlay(base, target).await
    }
}

impl<T: VolumeStore + ?Sized> VfsStoreExt for T {}

#[async_trait]
pub trait VfsArtifactStoreExt: VolumeStore {
    /// Export a volume cut as a single-file durable artifact.
    async fn export_volume_artifact(
        &self,
        source: crate::CloneVolumeSource,
    ) -> Result<Vec<u8>, VfsError> {
        let export = self.export_volume(source).await?;
        export.to_artifact_bytes()
    }

    /// Import a previously exported durable artifact into a new base volume.
    async fn import_volume_artifact(
        &self,
        bytes: &[u8],
        target: VolumeConfig,
    ) -> Result<Arc<dyn Volume>, VfsError> {
        let export = VolumeExport::from_artifact_bytes(bytes)?;
        self.import_volume(export, target).await
    }
}

impl<T: VolumeStore + ?Sized> VfsArtifactStoreExt for T {}

#[async_trait]
pub trait VfsVolumeExt: Volume {
    /// Capture a snapshot at the current visible cut.
    async fn visible_snapshot(&self) -> Result<Arc<dyn VolumeSnapshot>, VfsError> {
        self.snapshot(SnapshotOptions::default()).await
    }

    /// Capture a snapshot at the current durable cut.
    async fn durable_snapshot(&self) -> Result<Arc<dyn VolumeSnapshot>, VfsError> {
        self.snapshot(SnapshotOptions { durable: true }).await
    }

    /// Stream visible activity entries after `cursor`.
    async fn visible_activity_since(&self, cursor: LogCursor) -> Result<ActivityStream, VfsError> {
        self.activity_since(cursor, ActivityOptions::default())
            .await
    }

    /// Stream durable activity entries after `cursor`.
    async fn durable_activity_since(&self, cursor: LogCursor) -> Result<ActivityStream, VfsError> {
        self.activity_since(
            cursor,
            ActivityOptions {
                durable: true,
                ..Default::default()
            },
        )
        .await
    }

    /// Subscribe to visible activity watermark advances.
    fn subscribe_visible_activity(&self) -> ActivityReceiver {
        self.subscribe_activity(ActivityOptions::default())
    }

    /// Subscribe to durable activity watermark advances.
    fn subscribe_durable_activity(&self) -> ActivityReceiver {
        self.subscribe_activity(ActivityOptions {
            durable: true,
            ..Default::default()
        })
    }
}

impl<T: Volume + ?Sized> VfsVolumeExt for T {}
