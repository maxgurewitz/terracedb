use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb::Timestamp;
use terracedb_vfs::{DirEntry, FileKind, JsonValue, Stats, VfsError, VolumeId};

use crate::{SandboxError, SandboxSession, SandboxSessionState};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadonlyViewNodeKind {
    File,
    Directory,
    Symlink,
}

impl From<FileKind> for ReadonlyViewNodeKind {
    fn from(value: FileKind) -> Self {
        match value {
            FileKind::File => Self::File,
            FileKind::Directory => Self::Directory,
            FileKind::Symlink => Self::Symlink,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewStat {
    pub location: ReadonlyViewLocation,
    pub uri: String,
    pub kind: ReadonlyViewNodeKind,
    pub size: u64,
    pub modified_at: Timestamp,
}

impl ReadonlyViewStat {
    fn from_stats(location: ReadonlyViewLocation, stats: Stats) -> Self {
        let uri = location.to_uri();
        Self {
            location,
            uri,
            kind: stats.kind.into(),
            size: stats.size,
            modified_at: stats.modified_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewDirectoryEntry {
    pub name: String,
    pub location: ReadonlyViewLocation,
    pub uri: String,
    pub kind: ReadonlyViewNodeKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewSessionSummary {
    pub session_volume_id: VolumeId,
    pub workspace_root: String,
    pub state: SandboxSessionState,
    pub label: String,
    pub provider: String,
    pub active_view_handles: usize,
    pub available_cuts: Vec<ReadonlyViewCut>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadonlyViewReconnectRequest {
    pub session_volume_id: VolumeId,
    pub cut: ReadonlyViewCut,
    pub path: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadonlyViewProtocolRequest {
    ListSessions,
    RefreshSessions,
    ListHandles {
        session_volume_id: Option<VolumeId>,
    },
    OpenView {
        session_volume_id: VolumeId,
        request: ReadonlyViewRequest,
    },
    ReconnectView {
        request: ReadonlyViewReconnectRequest,
    },
    CloseView {
        session_volume_id: VolumeId,
        handle_id: String,
    },
    Stat {
        location: ReadonlyViewLocation,
    },
    ReadFile {
        location: ReadonlyViewLocation,
    },
    ReadDir {
        location: ReadonlyViewLocation,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadonlyViewProtocolResponse {
    Sessions {
        sessions: Vec<ReadonlyViewSessionSummary>,
    },
    Handles {
        handles: Vec<ReadonlyViewHandle>,
    },
    View {
        handle: ReadonlyViewHandle,
    },
    Stat {
        stat: Option<ReadonlyViewStat>,
    },
    File {
        bytes: Option<Vec<u8>>,
    },
    Directory {
        entries: Vec<ReadonlyViewDirectoryEntry>,
    },
    Ack,
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
        validate_location(session, location).await?;
        let fs = session.readonly_fs(location.cut).await?;
        fs.stat(&location.path).await.map_err(Into::into)
    }

    async fn read_file(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        validate_location(session, location).await?;
        let fs = session.readonly_fs(location.cut).await?;
        fs.read_file(&location.path).await.map_err(Into::into)
    }

    async fn readdir(
        &self,
        session: &SandboxSession,
        location: &ReadonlyViewLocation,
    ) -> Result<Vec<DirEntry>, SandboxError> {
        validate_location(session, location).await?;
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

#[async_trait]
pub trait ReadonlyViewSessionRegistry: Send + Sync {
    async fn list_sessions(&self) -> Result<Vec<SandboxSession>, SandboxError>;
    async fn get_session(
        &self,
        session_volume_id: VolumeId,
    ) -> Result<Option<SandboxSession>, SandboxError>;
}

#[derive(Clone, Default)]
pub struct StaticReadonlyViewRegistry {
    sessions: Arc<Vec<SandboxSession>>,
}

impl StaticReadonlyViewRegistry {
    pub fn new<I>(sessions: I) -> Self
    where
        I: IntoIterator<Item = SandboxSession>,
    {
        Self {
            sessions: Arc::new(sessions.into_iter().collect()),
        }
    }
}

#[async_trait]
impl ReadonlyViewSessionRegistry for StaticReadonlyViewRegistry {
    async fn list_sessions(&self) -> Result<Vec<SandboxSession>, SandboxError> {
        Ok(self.sessions.iter().cloned().collect())
    }

    async fn get_session(
        &self,
        session_volume_id: VolumeId,
    ) -> Result<Option<SandboxSession>, SandboxError> {
        for session in self.sessions.iter() {
            if session.info().await.session_volume_id == session_volume_id {
                return Ok(Some(session.clone()));
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
pub struct ReadonlyViewService<R> {
    registry: Arc<R>,
}

impl<R> ReadonlyViewService<R>
where
    R: ReadonlyViewSessionRegistry + 'static,
{
    pub fn new(registry: Arc<R>) -> Self {
        Self { registry }
    }

    pub async fn list_sessions(&self) -> Result<Vec<ReadonlyViewSessionSummary>, SandboxError> {
        let mut summaries = Vec::new();
        for session in self.registry.list_sessions().await? {
            let info = session.info().await;
            summaries.push(ReadonlyViewSessionSummary {
                session_volume_id: info.session_volume_id,
                workspace_root: info.workspace_root.clone(),
                state: info.state,
                label: format!("{} {}", info.session_volume_id, info.workspace_root),
                provider: info.services.readonly_view_provider.clone(),
                active_view_handles: info.provenance.active_view_handles.len(),
                available_cuts: vec![ReadonlyViewCut::Visible, ReadonlyViewCut::Durable],
            });
        }
        summaries.sort_by(|left, right| left.session_volume_id.cmp(&right.session_volume_id));
        Ok(summaries)
    }

    pub async fn refresh_sessions(&self) -> Result<Vec<ReadonlyViewSessionSummary>, SandboxError> {
        self.list_sessions().await
    }

    pub async fn list_handles(
        &self,
        session_volume_id: Option<VolumeId>,
    ) -> Result<Vec<ReadonlyViewHandle>, SandboxError> {
        let mut handles = Vec::new();
        let sessions = match session_volume_id {
            Some(session_volume_id) => vec![self.require_session(session_volume_id).await?],
            None => self.registry.list_sessions().await?,
        };
        for session in sessions {
            handles.extend(session.active_readonly_views().await);
        }
        handles.sort_by(|left, right| left.handle_id.cmp(&right.handle_id));
        Ok(handles)
    }

    pub async fn open_view(
        &self,
        session_volume_id: VolumeId,
        request: ReadonlyViewRequest,
    ) -> Result<ReadonlyViewHandle, SandboxError> {
        self.require_session(session_volume_id)
            .await?
            .open_readonly_view(request)
            .await
    }

    pub async fn reconnect_view(
        &self,
        request: ReadonlyViewReconnectRequest,
    ) -> Result<ReadonlyViewHandle, SandboxError> {
        self.require_session(request.session_volume_id)
            .await?
            .open_readonly_view(ReadonlyViewRequest {
                cut: request.cut,
                path: request.path,
                label: request.label,
            })
            .await
    }

    pub async fn close_view(
        &self,
        session_volume_id: VolumeId,
        handle_id: &str,
    ) -> Result<(), SandboxError> {
        self.require_session(session_volume_id)
            .await?
            .close_readonly_view(handle_id)
            .await
    }

    pub async fn stat(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<ReadonlyViewStat>, SandboxError> {
        let session = self.require_session(location.session_volume_id).await?;
        Ok(session
            .stat_readonly_view(location)
            .await?
            .map(|stats| ReadonlyViewStat::from_stats(location.clone(), stats)))
    }

    pub async fn read_file(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        self.require_session(location.session_volume_id)
            .await?
            .read_file_readonly_view(location)
            .await
    }

    pub async fn readdir(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Vec<ReadonlyViewDirectoryEntry>, SandboxError> {
        let session = self.require_session(location.session_volume_id).await?;
        let mut entries = match session.readdir_readonly_view(location).await {
            Ok(entries) => entries,
            Err(SandboxError::Vfs(VfsError::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error),
        };
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries
            .into_iter()
            .map(|entry| {
                let child_location = ReadonlyViewLocation {
                    session_volume_id: location.session_volume_id,
                    cut: location.cut,
                    path: if location.path == "/" {
                        format!("/{}", entry.name)
                    } else {
                        format!("{}/{}", location.path.trim_end_matches('/'), entry.name)
                    },
                };
                ReadonlyViewDirectoryEntry {
                    name: entry.name,
                    uri: child_location.to_uri(),
                    kind: entry.kind.into(),
                    location: child_location,
                }
            })
            .collect())
    }

    pub async fn handle_protocol(
        &self,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError> {
        match request {
            ReadonlyViewProtocolRequest::ListSessions => {
                Ok(ReadonlyViewProtocolResponse::Sessions {
                    sessions: self.list_sessions().await?,
                })
            }
            ReadonlyViewProtocolRequest::RefreshSessions => {
                Ok(ReadonlyViewProtocolResponse::Sessions {
                    sessions: self.refresh_sessions().await?,
                })
            }
            ReadonlyViewProtocolRequest::ListHandles { session_volume_id } => {
                Ok(ReadonlyViewProtocolResponse::Handles {
                    handles: self.list_handles(session_volume_id).await?,
                })
            }
            ReadonlyViewProtocolRequest::OpenView {
                session_volume_id,
                request,
            } => Ok(ReadonlyViewProtocolResponse::View {
                handle: self.open_view(session_volume_id, request).await?,
            }),
            ReadonlyViewProtocolRequest::ReconnectView { request } => {
                Ok(ReadonlyViewProtocolResponse::View {
                    handle: self.reconnect_view(request).await?,
                })
            }
            ReadonlyViewProtocolRequest::CloseView {
                session_volume_id,
                handle_id,
            } => {
                self.close_view(session_volume_id, &handle_id).await?;
                Ok(ReadonlyViewProtocolResponse::Ack)
            }
            ReadonlyViewProtocolRequest::Stat { location } => {
                Ok(ReadonlyViewProtocolResponse::Stat {
                    stat: self.stat(&location).await?,
                })
            }
            ReadonlyViewProtocolRequest::ReadFile { location } => {
                Ok(ReadonlyViewProtocolResponse::File {
                    bytes: self.read_file(&location).await?,
                })
            }
            ReadonlyViewProtocolRequest::ReadDir { location } => {
                Ok(ReadonlyViewProtocolResponse::Directory {
                    entries: self.readdir(&location).await?,
                })
            }
        }
    }

    pub async fn handle_protocol_json(&self, payload: &[u8]) -> Result<Vec<u8>, SandboxError> {
        let request = serde_json::from_slice::<ReadonlyViewProtocolRequest>(payload)?;
        let response = self.handle_protocol(request).await?;
        serde_json::to_vec(&response).map_err(Into::into)
    }

    async fn require_session(
        &self,
        session_volume_id: VolumeId,
    ) -> Result<SandboxSession, SandboxError> {
        self.registry
            .get_session(session_volume_id)
            .await?
            .ok_or(SandboxError::ReadonlyViewSessionNotFound { session_volume_id })
    }
}

#[async_trait]
pub trait ReadonlyViewProtocolTransport: Send + Sync {
    async fn send(
        &self,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError>;
    async fn refresh(&self) -> Result<(), SandboxError>;
    async fn reconnect(&self) -> Result<(), SandboxError>;
}

#[derive(Clone)]
pub struct LocalReadonlyViewBridge<R> {
    service: Arc<ReadonlyViewService<R>>,
}

impl<R> LocalReadonlyViewBridge<R>
where
    R: ReadonlyViewSessionRegistry + 'static,
{
    pub fn new(service: Arc<ReadonlyViewService<R>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<R> ReadonlyViewProtocolTransport for LocalReadonlyViewBridge<R>
where
    R: ReadonlyViewSessionRegistry + 'static,
{
    async fn send(
        &self,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError> {
        self.service.handle_protocol(request).await
    }

    async fn refresh(&self) -> Result<(), SandboxError> {
        self.service.refresh_sessions().await?;
        Ok(())
    }

    async fn reconnect(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

#[async_trait]
pub trait ReadonlyViewRemoteEndpoint: Send + Sync {
    async fn send_authenticated(
        &self,
        token: &str,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError>;
    async fn refresh_authenticated(&self, token: &str) -> Result<(), SandboxError>;
    async fn reconnect_authenticated(&self, token: &str) -> Result<(), SandboxError>;
}

#[derive(Clone)]
pub struct AuthenticatedReadonlyViewRemoteEndpoint<R> {
    service: Arc<ReadonlyViewService<R>>,
    expected_token: Arc<str>,
}

impl<R> AuthenticatedReadonlyViewRemoteEndpoint<R>
where
    R: ReadonlyViewSessionRegistry + 'static,
{
    pub fn new(service: Arc<ReadonlyViewService<R>>, expected_token: impl Into<String>) -> Self {
        Self {
            service,
            expected_token: Arc::from(expected_token.into()),
        }
    }

    fn authorize(&self, token: &str) -> Result<(), SandboxError> {
        if token == self.expected_token.as_ref() {
            Ok(())
        } else {
            Err(SandboxError::ReadonlyViewUnauthorized)
        }
    }
}

#[async_trait]
impl<R> ReadonlyViewRemoteEndpoint for AuthenticatedReadonlyViewRemoteEndpoint<R>
where
    R: ReadonlyViewSessionRegistry + 'static,
{
    async fn send_authenticated(
        &self,
        token: &str,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError> {
        self.authorize(token)?;
        self.service.handle_protocol(request).await
    }

    async fn refresh_authenticated(&self, token: &str) -> Result<(), SandboxError> {
        self.authorize(token)?;
        self.service.refresh_sessions().await?;
        Ok(())
    }

    async fn reconnect_authenticated(&self, token: &str) -> Result<(), SandboxError> {
        self.authorize(token)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RemoteReadonlyViewBridge<E> {
    endpoint: Arc<E>,
    token: Arc<str>,
}

impl<E> RemoteReadonlyViewBridge<E>
where
    E: ReadonlyViewRemoteEndpoint + 'static,
{
    pub fn new(endpoint: Arc<E>, token: impl Into<String>) -> Self {
        Self {
            endpoint,
            token: Arc::from(token.into()),
        }
    }
}

#[async_trait]
impl<E> ReadonlyViewProtocolTransport for RemoteReadonlyViewBridge<E>
where
    E: ReadonlyViewRemoteEndpoint + 'static,
{
    async fn send(
        &self,
        request: ReadonlyViewProtocolRequest,
    ) -> Result<ReadonlyViewProtocolResponse, SandboxError> {
        self.endpoint
            .send_authenticated(self.token.as_ref(), request)
            .await
    }

    async fn refresh(&self) -> Result<(), SandboxError> {
        self.endpoint
            .refresh_authenticated(self.token.as_ref())
            .await
    }

    async fn reconnect(&self) -> Result<(), SandboxError> {
        self.endpoint
            .reconnect_authenticated(self.token.as_ref())
            .await
    }
}

async fn validate_location(
    session: &SandboxSession,
    location: &ReadonlyViewLocation,
) -> Result<(), SandboxError> {
    let session_volume_id = session.info().await.session_volume_id;
    if session_volume_id == location.session_volume_id {
        Ok(())
    } else {
        Err(SandboxError::ReadonlyViewSessionNotFound {
            session_volume_id: location.session_volume_id,
        })
    }
}
