//! Deterministic HTTP helpers for turmoil-based Terracedb simulations.
//!
//! The core harness exposes a reusable simulated HTTP server and client on top
//! of `terracedb-simulation`. It keeps the low-level surface in terms of
//! `hyper` request and response primitives while layering in deterministic
//! tracing and traffic controls that are useful for debugging and fault
//! injection.
//!
//! Common simulation patterns:
//!
//! - capture request/response traces with [`SimulatedHttpTrace`]
//! - delay handlers with `tokio::time::sleep` to model slow services
//! - partition or hold traffic with [`SimulatedHttpServer::partition_from`] and
//!   [`SimulatedHttpServer::hold_from`]
//! - run retries against the same host and port to exercise restart behavior
//!
//! Enable the `axum` feature to serve an `axum::Router` through the same
//! harness via [`axum_router_server`] or
//! [`axum_router_server_with_shutdown`].

use std::{
    error::Error as StdError,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Body, Incoming},
    client::conn::http1,
    header::{HOST, HeaderValue},
    server::conn::http1 as server_http1,
};
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
use terracedb_simulation::SimulationHost;
use tokio::{sync::watch, task::JoinSet};
use turmoil::net::{TcpListener, TcpStream};

const DEFAULT_CONNECT_ATTEMPTS: usize = 100;
const DEFAULT_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(5);
const DRIVER_HOST: &str = "driver";

type BoxError = Box<dyn StdError + Send + Sync + 'static>;
type ServerFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;
type BoxedResponseBody = UnsyncBoxBody<Bytes, BoxError>;
type BoxedHandlerFuture =
    Pin<Box<dyn Future<Output = Result<Response<BoxedResponseBody>, BoxError>> + Send + 'static>>;
type BoxedHandler = Arc<dyn Fn(Request<Incoming>) -> BoxedHandlerFuture + Send + Sync + 'static>;

fn lock_trace(trace: &Mutex<Vec<HttpTraceEvent>>) -> MutexGuard<'_, Vec<HttpTraceEvent>> {
    trace
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn request_target(request: &Request<impl Sized>) -> String {
    request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpFailureStage {
    Connect,
    Handshake,
    Send,
    ReceiveBody,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpTraceEvent {
    ServerListening {
        host: String,
        port: u16,
    },
    ServerConnectionAccepted {
        host: String,
        port: u16,
    },
    ServerRequestReceived {
        method: String,
        path: String,
    },
    ServerResponseSent {
        status: u16,
    },
    ServerRequestFailed {
        message: String,
    },
    ServerShutdown {
        host: String,
        port: u16,
    },
    ClientConnectAttempt {
        host: String,
        port: u16,
        attempt: usize,
    },
    ClientConnected {
        host: String,
        port: u16,
    },
    ClientRequestSent {
        method: String,
        path: String,
    },
    ClientResponseReceived {
        status: u16,
        body_len: usize,
    },
    ClientRequestFailed {
        stage: HttpFailureStage,
        kind: io::ErrorKind,
        message: String,
    },
}

#[derive(Clone, Default)]
pub struct SimulatedHttpTrace {
    inner: Arc<Mutex<Vec<HttpTraceEvent>>>,
}

impl std::fmt::Debug for SimulatedHttpTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulatedHttpTrace")
            .field("event_count", &self.events().len())
            .finish()
    }
}

impl SimulatedHttpTrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<HttpTraceEvent> {
        lock_trace(&self.inner).clone()
    }

    pub fn record(&self, event: HttpTraceEvent) {
        lock_trace(&self.inner).push(event);
    }
}

#[derive(Clone)]
pub struct SimulatedHttpServer {
    host: Arc<str>,
    port: u16,
    trace: SimulatedHttpTrace,
    run: Arc<dyn Fn() -> ServerFuture + Send + Sync>,
}

impl std::fmt::Debug for SimulatedHttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulatedHttpServer")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("trace", &self.trace)
            .finish()
    }
}

impl SimulatedHttpServer {
    pub fn new<Handler, Fut, ResponseBody, Error>(
        host: impl Into<Arc<str>>,
        port: u16,
        handler: Handler,
    ) -> Self
    where
        Handler: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<ResponseBody>, Error>> + Send + 'static,
        ResponseBody: Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: StdError + Send + Sync + 'static,
        Error: StdError + Send + Sync + 'static,
    {
        Self::build(host.into(), port, None, SimulatedHttpTrace::new(), handler)
    }

    pub fn with_shutdown<Handler, Fut, ResponseBody, Error>(
        host: impl Into<Arc<str>>,
        port: u16,
        shutdown: watch::Receiver<bool>,
        handler: Handler,
    ) -> Self
    where
        Handler: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<ResponseBody>, Error>> + Send + 'static,
        ResponseBody: Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: StdError + Send + Sync + 'static,
        Error: StdError + Send + Sync + 'static,
    {
        Self::build(
            host.into(),
            port,
            Some(shutdown),
            SimulatedHttpTrace::new(),
            handler,
        )
    }

    fn build<Handler, Fut, ResponseBody, Error>(
        host: Arc<str>,
        port: u16,
        shutdown: Option<watch::Receiver<bool>>,
        trace: SimulatedHttpTrace,
        handler: Handler,
    ) -> Self
    where
        Handler: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<ResponseBody>, Error>> + Send + 'static,
        ResponseBody: Body<Data = Bytes> + Send + 'static,
        ResponseBody::Error: StdError + Send + Sync + 'static,
        Error: StdError + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let boxed_handler: BoxedHandler = Arc::new(move |request| {
            let handler = handler.clone();
            Box::pin(async move {
                let response = (handler)(request)
                    .await
                    .map_err(|error| -> BoxError { Box::new(error) })?;
                Ok(response.map(|body| {
                    body.map_err(|error| -> BoxError { Box::new(error) })
                        .boxed_unsync()
                }))
            })
        });

        let run = Arc::new({
            let host = host.clone();
            let trace = trace.clone();
            let boxed_handler = boxed_handler.clone();
            move || {
                let host = host.clone();
                let trace = trace.clone();
                let boxed_handler = boxed_handler.clone();
                let shutdown = shutdown.clone();
                Box::pin(async move {
                    run_http_server(host, port, trace, shutdown, boxed_handler).await
                }) as ServerFuture
            }
        });

        Self {
            host,
            port,
            trace,
            run,
        }
    }

    pub fn host(&self) -> &str {
        self.host.as_ref()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn trace(&self) -> SimulatedHttpTrace {
        self.trace.clone()
    }

    pub async fn run(&self) -> io::Result<()> {
        (self.run)().await
    }

    pub fn simulation_host(&self) -> SimulationHost {
        let host = self.host.clone();
        let server = self.clone();
        SimulationHost::new(host, move || {
            let server = server.clone();
            async move { server.run().await.map_err(Into::into) }
        })
    }

    pub fn hold_from(&self, peer_host: &str) {
        turmoil::hold(peer_host, self.host.as_ref());
    }

    pub fn release_from(&self, peer_host: &str) {
        turmoil::release(peer_host, self.host.as_ref());
    }

    pub fn partition_from(&self, peer_host: &str) {
        turmoil::partition(peer_host, self.host.as_ref());
    }

    pub fn hold_from_driver(&self) {
        self.hold_from(DRIVER_HOST);
    }

    pub fn release_from_driver(&self) {
        self.release_from(DRIVER_HOST);
    }

    pub fn partition_from_driver(&self) {
        self.partition_from(DRIVER_HOST);
    }
}

#[derive(Clone, Debug)]
pub struct SimulatedHttpClient {
    host: Arc<str>,
    port: u16,
    connect_attempts: usize,
    connect_retry_delay: Duration,
    request_timeout: Option<Duration>,
    trace: SimulatedHttpTrace,
}

impl SimulatedHttpClient {
    pub fn new(host: impl Into<Arc<str>>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            connect_attempts: DEFAULT_CONNECT_ATTEMPTS,
            connect_retry_delay: DEFAULT_CONNECT_RETRY_DELAY,
            request_timeout: None,
            trace: SimulatedHttpTrace::new(),
        }
    }

    pub fn with_connect_retries(mut self, attempts: usize, retry_delay: Duration) -> Self {
        self.connect_attempts = attempts.max(1);
        self.connect_retry_delay = retry_delay;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn trace(&self) -> SimulatedHttpTrace {
        self.trace.clone()
    }

    pub async fn send<RequestBody>(
        &self,
        request: Request<RequestBody>,
    ) -> io::Result<Response<Bytes>>
    where
        Bytes: From<RequestBody>,
    {
        let send_request = async {
            let stream = self.connect().await?;
            let io = TokioIo::new(stream);
            let (mut sender, connection) = http1::handshake(io)
                .await
                .map_err(|error| io::Error::other(format!("http handshake failed: {error}")))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut request = request.map(Bytes::from);
            ensure_host_header(&self.host, self.port, &mut request)?;
            self.trace.record(HttpTraceEvent::ClientRequestSent {
                method: request.method().as_str().to_string(),
                path: request_target(&request),
            });

            let response = sender
                .send_request(request.map(Full::new))
                .await
                .map_err(io::Error::other)?;
            let status = response.status();
            let (parts, body) = response.into_parts();
            let body = body.collect().await.map_err(io::Error::other)?.to_bytes();
            self.trace.record(HttpTraceEvent::ClientResponseReceived {
                status: status.as_u16(),
                body_len: body.len(),
            });
            Ok(Response::from_parts(parts, body))
        };

        let result = match self.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, send_request)
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "timed out waiting for simulated http response from {}:{}",
                            self.host, self.port
                        ),
                    )
                })?,
            None => send_request.await,
        };

        if let Err(error) = &result {
            self.trace.record(HttpTraceEvent::ClientRequestFailed {
                stage: classify_failure_stage(error),
                kind: error.kind(),
                message: error.to_string(),
            });
        }
        result
    }

    pub async fn request_json<RequestBody, ResponseBody>(
        &self,
        method: Method,
        path: &str,
        payload: Option<&RequestBody>,
    ) -> io::Result<(StatusCode, ResponseBody)>
    where
        RequestBody: Serialize,
        ResponseBody: DeserializeOwned,
    {
        let body = match payload {
            Some(value) => Bytes::from(
                serde_json::to_vec(value)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            ),
            None => Bytes::new(),
        };

        let mut builder = Request::builder().method(method).uri(path);
        if payload.is_some() {
            builder = builder.header("content-type", "application/json");
        }

        let request = builder
            .body(body)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let response = self.send(request).await?;
        let status = response.status();
        let decoded = serde_json::from_slice(response.body())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok((status, decoded))
    }

    async fn connect(&self) -> io::Result<TcpStream> {
        let mut last_error = None;
        for attempt in 1..=self.connect_attempts.max(1) {
            self.trace.record(HttpTraceEvent::ClientConnectAttempt {
                host: self.host.to_string(),
                port: self.port,
                attempt,
            });
            match TcpStream::connect((self.host.as_ref(), self.port)).await {
                Ok(stream) => {
                    self.trace.record(HttpTraceEvent::ClientConnected {
                        host: self.host.to_string(),
                        port: self.port,
                    });
                    return Ok(stream);
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    last_error = Some(error);
                    tokio::time::sleep(self.connect_retry_delay).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("{}:{}", self.host, self.port),
            )
        }))
    }
}

fn classify_failure_stage(error: &io::Error) -> HttpFailureStage {
    match error.kind() {
        io::ErrorKind::TimedOut => HttpFailureStage::Timeout,
        io::ErrorKind::ConnectionRefused => HttpFailureStage::Connect,
        _ => HttpFailureStage::Send,
    }
}

fn ensure_host_header(
    host: &str,
    port: u16,
    request: &mut Request<Bytes>,
) -> Result<(), io::Error> {
    if request.headers().contains_key(HOST) {
        return Ok(());
    }

    let host_header = HeaderValue::from_str(&format!("{host}:{port}"))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    request.headers_mut().insert(HOST, host_header);
    Ok(())
}

async fn run_http_server(
    host: Arc<str>,
    port: u16,
    trace: SimulatedHttpTrace,
    mut shutdown: Option<watch::Receiver<bool>>,
    handler: BoxedHandler,
) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    trace.record(HttpTraceEvent::ServerListening {
        host: host.to_string(),
        port,
    });
    let mut connections = JoinSet::new();

    loop {
        match shutdown.as_mut() {
            Some(shutdown) => {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let (stream, _peer) = accepted?;
                        spawn_http_connection(&mut connections, host.clone(), port, trace.clone(), handler.clone(), stream);
                    }
                }
            }
            None => {
                let (stream, _peer) = listener.accept().await?;
                spawn_http_connection(
                    &mut connections,
                    host.clone(),
                    port,
                    trace.clone(),
                    handler.clone(),
                    stream,
                );
            }
        }
    }

    trace.record(HttpTraceEvent::ServerShutdown {
        host: host.to_string(),
        port,
    });
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(io::Error::other(error.to_string())),
        }
    }
    Ok(())
}

fn spawn_http_connection(
    connections: &mut JoinSet<io::Result<()>>,
    host: Arc<str>,
    port: u16,
    trace: SimulatedHttpTrace,
    handler: BoxedHandler,
    stream: TcpStream,
) {
    trace.record(HttpTraceEvent::ServerConnectionAccepted {
        host: host.to_string(),
        port,
    });
    connections.spawn(async move {
        let io = TokioIo::new(stream);
        let connection_trace = trace.clone();
        let service = hyper::service::service_fn(move |request: Request<Incoming>| {
            let trace = connection_trace.clone();
            let handler = handler.clone();
            async move {
                trace.record(HttpTraceEvent::ServerRequestReceived {
                    method: request.method().as_str().to_string(),
                    path: request_target(&request),
                });
                let response = (handler)(request).await;
                match &response {
                    Ok(response) => trace.record(HttpTraceEvent::ServerResponseSent {
                        status: response.status().as_u16(),
                    }),
                    Err(error) => trace.record(HttpTraceEvent::ServerRequestFailed {
                        message: error.to_string(),
                    }),
                }
                response.map_err(|error| io::Error::other(error.to_string()))
            }
        });

        if let Err(error) = server_http1::Builder::new()
            .serve_connection(io, service)
            .await
        {
            trace.record(HttpTraceEvent::ServerRequestFailed {
                message: error.to_string(),
            });
        }
        Ok(())
    });
}

#[cfg(feature = "axum")]
pub fn axum_router_server(
    host: impl Into<Arc<str>>,
    port: u16,
    router: axum::Router,
) -> SimulatedHttpServer {
    use std::convert::Infallible;

    use tower::ServiceExt;

    SimulatedHttpServer::new(host, port, move |request| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(request)
                .await
                .map_err(|never: Infallible| never)?;
            Ok::<Response<axum::body::Body>, Infallible>(response)
        }
    })
}

#[cfg(feature = "axum")]
pub fn axum_router_server_with_shutdown(
    host: impl Into<Arc<str>>,
    port: u16,
    router: axum::Router,
    shutdown: watch::Receiver<bool>,
) -> SimulatedHttpServer {
    use std::convert::Infallible;

    use tower::ServiceExt;

    SimulatedHttpServer::with_shutdown(host, port, shutdown, move |request| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(request)
                .await
                .map_err(|never: Infallible| never)?;
            Ok::<Response<axum::body::Body>, Infallible>(response)
        }
    })
}
