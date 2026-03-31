use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::Timestamp;
use terracedb_vfs::{DirEntry, JsonValue, Stats, VolumeId};

use crate::{SandboxError, SandboxSession};

pub const READONLY_VIEW_URI_SCHEME: &str = "terrace-view";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadonlyViewCut {
    Visible,
    Durable,
}

impl ReadonlyViewCut {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::Durable => "durable",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "visible" => Some(Self::Visible),
            "durable" => Some(Self::Durable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewLocation {
    pub session_volume_id: VolumeId,
    pub cut: ReadonlyViewCut,
    pub path: String,
}

impl ReadonlyViewLocation {
    pub fn to_uri(&self) -> String {
        format!(
            "{READONLY_VIEW_URI_SCHEME}://session/{}/{}/{}",
            self.session_volume_id,
            self.cut.as_str(),
            self.path.trim_start_matches('/')
        )
    }

    pub fn parse(uri: &str) -> Result<Self, SandboxError> {
        let prefix = format!("{READONLY_VIEW_URI_SCHEME}://session/");
        let rest =
            uri.strip_prefix(&prefix)
                .ok_or_else(|| SandboxError::InvalidReadonlyViewUri {
                    uri: uri.to_string(),
                })?;
        let mut parts = rest.splitn(3, '/');
        let session_volume_id =
            parts
                .next()
                .ok_or_else(|| SandboxError::InvalidReadonlyViewUri {
                    uri: uri.to_string(),
                })?;
        let cut = parts
            .next()
            .and_then(ReadonlyViewCut::parse)
            .ok_or_else(|| SandboxError::InvalidReadonlyViewUri {
                uri: uri.to_string(),
            })?;
        let path_suffix = parts
            .next()
            .ok_or_else(|| SandboxError::InvalidReadonlyViewUri {
                uri: uri.to_string(),
            })?;
        let value = u128::from_str_radix(session_volume_id, 16).map_err(|_| {
            SandboxError::InvalidReadonlyViewUri {
                uri: uri.to_string(),
            }
        })?;
        Ok(Self {
            session_volume_id: VolumeId::new(value),
            cut,
            path: format!("/{}", path_suffix),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewRequest {
    pub cut: ReadonlyViewCut,
    pub path: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadonlyViewHandle {
    pub handle_id: String,
    pub provider: String,
    pub label: String,
    pub location: ReadonlyViewLocation,
    pub uri: String,
    pub opened_at: Timestamp,
    pub metadata: BTreeMap<String, JsonValue>,
}

#[async_trait]
pub trait ReadonlyViewProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn open_view(
        &self,
        session: &SandboxSession,
        request: ReadonlyViewRequest,
    ) -> Result<ReadonlyViewHandle, SandboxError>;
    async fn stat(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Stats>, SandboxError>;
    async fn read_file(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Vec<u8>>, SandboxError>;
    async fn readdir(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Vec<DirEntry>, SandboxError>;
    async fn close_view(
        &self,
        session: &SandboxSession,
        handle: &ReadonlyViewHandle,
    ) -> Result<(), SandboxError>;
}

#[derive(Clone, Debug)]
pub struct DeterministicReadonlyViewProvider {
    name: String,
}

impl DeterministicReadonlyViewProvider {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for DeterministicReadonlyViewProvider {
    fn default() -> Self {
        Self::new("deterministic-view")
    }
}

#[async_trait]
impl ReadonlyViewProvider for DeterministicReadonlyViewProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn open_view(
        &self,
        session: &SandboxSession,
        request: ReadonlyViewRequest,
    ) -> Result<ReadonlyViewHandle, SandboxError> {
        let info = session.info().await;
        let label = request
            .label
            .clone()
            .unwrap_or_else(|| format!("{} {}", request.cut.as_str(), request.path));
        let location = ReadonlyViewLocation {
            session_volume_id: info.session_volume_id,
            cut: request.cut,
            path: request.path.clone(),
        };
        let uri = location.to_uri();
        let handle_id = format!(
            "{}:{}:{}:{}",
            self.name,
            info.session_volume_id,
            request.cut.as_str(),
            request.path.replace('/', "_")
        );
        Ok(ReadonlyViewHandle {
            handle_id,
            provider: self.name.clone(),
            label,
            location,
            uri,
            opened_at: info.updated_at,
            metadata: BTreeMap::new(),
        })
    }

    async fn stat(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Stats>, SandboxError> {
        let fs = session.readonly_fs(location.cut).await?;
        fs.stat(&location.path).await.map_err(Into::into)
    }

    async fn read_file(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        let fs = session.readonly_fs(location.cut).await?;
        fs.read_file(&location.path).await.map_err(Into::into)
    }

    async fn readdir(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Vec<DirEntry>, SandboxError> {
        let fs = session.readonly_fs(location.cut).await?;
        fs.readdir(&location.path).await.map_err(Into::into)
    }

    async fn close_view(
        &self,
        _session: &SandboxSession,
        _handle: &ReadonlyViewHandle,
    ) -> Result<(), SandboxError> {
        Ok(())
    }
}
