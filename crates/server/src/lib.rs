//! A single TCP/TLS listener dispatching gRPC and Triple HTTP Unary requests.
//! Bindings remain independent; auto protocol selection belongs to hyper-util.

use bytes::Bytes;
use http::{Request, Response, StatusCode, Version, header};
use http_body_util::{Either, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use rpc::{CancellationToken, Limits, StreamExecutor};
use std::{error::Error as StdError, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::task::TaskTracker;

mod accept;

/// Unified service/body error for both bindings.
pub type BoxError = Box<dyn StdError + Send + Sync>;
/// The response body is either a streaming RPC or a complete HTTP Unary value.
pub type ResponseBody = Either<rpc::ResponseBody, Full<Bytes>>;

/// Listener, TLS or connection-driver failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// TCP accept or TLS handshake I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The connection driver returned a non-peer-termination error.
    #[error("connection: {0}")]
    Connection(BoxError),
    /// A tracked connection task panicked.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    /// A numeric configuration cannot be represented by hyper.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
}
/// Server result with owned connection errors.
pub type Result<T> = std::result::Result<T, Error>;
/// Synchronous observer for a connection or accept error.
pub type ConnectionErrorCallback = dyn Fn(&Error) + Send + Sync;

/// Configuration for both bindings and their shared network lifecycle.
#[derive(Clone)]
pub struct Config {
    /// RPC message/header/concurrency bounds.
    pub rpc_limits: Limits,
    /// HTTP Unary request and decompressed-message bounds.
    pub http: triple_http::Config,
    /// HTTP/1 header deadline, starting after auto protocol identification.
    pub header_read_timeout: Duration,
    /// Time allowed to drain each connection after shutdown.
    pub shutdown_timeout: Duration,
    /// Optional TLS configuration, cloned with h2 and http/1.1 ALPN.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Observes connection and accept errors, including errors recorded without
    /// failing the listener. The callback runs synchronously and should be short.
    pub on_connection_error: Option<Arc<ConnectionErrorCallback>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_limits: Limits::default(),
            http: triple_http::Config::default(),
            header_read_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
            tls: None,
            on_connection_error: None,
        }
    }
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rpc_limits", &self.rpc_limits)
            .field("http", &self.http)
            .field("header_read_timeout", &self.header_read_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("tls", &self.tls)
            .field(
                "on_connection_error",
                &self.on_connection_error.as_ref().map(|_| "callback"),
            )
            .finish()
    }
}

/// Owns both services and the shared connection configuration.
pub struct Server<S, H> {
    rpc: Arc<S>,
    http: Arc<H>,
    config: Config,
}
impl<S: rpc::Service + 'static, H: triple_http::UnaryHandler + 'static> Server<S, H> {
    /// Creates a server, advertising both supported protocols when TLS is enabled.
    pub fn new(rpc: Arc<S>, http: Arc<H>, mut config: Config) -> Self {
        if let Some(tls) = config.tls.take() {
            let mut tls = (*tls).clone();
            tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            config.tls = Some(Arc::new(tls));
        }
        Self { rpc, http, config }
    }

    /// Accepts connections until shutdown, then drains and joins every connection.
    /// Peer I/O termination and HTTP/1 header timeouts are recorded through the
    /// callback. Other connection errors are returned after all connections drain.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let mut connections = JoinSet::new();
        let mut result = Ok(());
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(joined) = joined { self.remember(&mut result, joined); }
                },
                accepted = listener.accept() => match accepted {
                    Ok((io, _)) => {
                        let server = self.clone(); let stop = shutdown.clone();
                        connections.spawn(async move { server.serve_connection(io, stop).await });
                    },
                    Err(error) => {
                        let retry = accept::is_transient(&error);
                        let error = Error::Io(error);
                        self.record(&error);
                        if !retry { result = Err(error); break; }
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                        }
                    }
                }
            }
        }
        shutdown.cancel();
        while let Some(joined) = connections.join_next().await {
            self.remember(&mut result, joined);
        }
        result
    }

    fn record(&self, error: &Error) {
        if let Some(callback) = &self.config.on_connection_error {
            callback(error);
        }
    }

    fn remember(
        &self,
        result: &mut Result<()>,
        joined: std::result::Result<Result<()>, tokio::task::JoinError>,
    ) {
        if let Err(error) = joined.map_err(Error::Task).and_then(|r| r) {
            self.record(&error);
            if !connection_is_peer_error(&error) && result.is_ok() {
                *result = Err(error);
            }
        }
    }

    /// Serves one TCP stream, optionally performing the configured TLS handshake.
    pub async fn serve_connection<T>(&self, io: T, shutdown: CancellationToken) -> Result<()>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if let Some(config) = &self.config.tls {
            let acceptor = TlsAcceptor::from(config.clone());
            let io = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                io = acceptor.accept(io) => io?,
            };
            self.serve_io(io, shutdown).await
        } else {
            self.serve_io(io, shutdown).await
        }
    }

    async fn serve_io<T>(&self, io: T, shutdown: CancellationToken) -> Result<()>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let tracker = TaskTracker::new();
        let stop = CancellationToken::new();
        let mut builder = hyper_util::server::conn::auto::Builder::new(StreamExecutor::new(
            tracker.clone(),
            stop.clone(),
        ));
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(self.config.header_read_timeout);
        let settings = rpc::http2_settings(self.config.rpc_limits);
        let headers = u32::try_from(settings.max_header_list_size)
            .map_err(|_| Error::InvalidConfig("header limit"))?;
        // Every supported numeric option comes from rpc's frozen configuration.
        // auto::Http2Builder lacks header_table_size. RFC 9113 §6.5.2 defaults
        // it to 4096, equal to rpc's frozen value; omit only this approved item.
        builder
            .http2()
            .timer(TokioTimer::new())
            .initial_stream_window_size(settings.initial_stream_window_size)
            .initial_connection_window_size(settings.initial_connection_window_size)
            .adaptive_window(false)
            .max_frame_size(settings.max_frame_size)
            .max_concurrent_streams(settings.max_concurrent_streams)
            .max_header_list_size(headers)
            .max_send_buf_size(settings.max_send_buf_size)
            .max_pending_accept_reset_streams(settings.max_pending_accept_reset_streams)
            .max_local_error_reset_streams(settings.max_local_error_reset_streams)
            .keep_alive_interval(settings.keep_alive_interval)
            .keep_alive_timeout(settings.keep_alive_timeout)
            .auto_date_header(false);
        // Rapid-reset protection remains enabled; extended CONNECT stays disabled.
        let (rpc, http, calls) = (self.rpc.clone(), self.http.clone(), tracker.clone());
        let binding = triple_http::Binding::new(self.config.http);
        let limits = self.config.rpc_limits;
        let connection = builder.serve_connection(
            TokioIo::new(io),
            hyper::service::service_fn(move |request| {
                dispatch(
                    request,
                    rpc.clone(),
                    http.clone(),
                    binding,
                    limits,
                    calls.clone(),
                )
            }),
        );
        tokio::pin!(connection);
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                connection.as_mut().graceful_shutdown();
                match tokio::time::Instant::now().checked_add(self.config.shutdown_timeout) {
                    Some(deadline) => match tokio::time::timeout_at(deadline, &mut connection).await {
                        Ok(result) => connection_result(result), Err(_) => Ok(()),
                    },
                    None => Err(Error::InvalidConfig("shutdown timeout overflow")),
                }
            },
            result = &mut connection => connection_result(result),
        };
        // Stop every stream task, close the tracker, and join before returning.
        stop.cancel();
        tracker.close();
        tracker.wait().await;
        result
    }
}

async fn dispatch<S: rpc::Service + 'static, H: triple_http::UnaryHandler>(
    request: Request<Incoming>,
    rpc: Arc<S>,
    http: Arc<H>,
    binding: triple_http::Binding,
    limits: Limits,
    tracker: TaskTracker,
) -> std::result::Result<Response<ResponseBody>, BoxError> {
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::trim)
        .unwrap_or("");
    if media.starts_with("application/grpc") || media == "application/triple+wrapper" {
        if request.version() != Version::HTTP_2 {
            // TODO(spec): Triple does not define gRPC requests over HTTP/1.
            let mut response = Response::new(Either::Right(Full::new(Bytes::new())));
            *response.status_mut() = StatusCode::HTTP_VERSION_NOT_SUPPORTED;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            return Ok(response);
        }
        // TODO(spec): Preserve rpc's rejection of +json and triple+wrapper.
        rpc::handle(request, rpc, limits, tracker)
            .await
            .map(|r| r.map(Either::Left))
            .map_err(|e| Box::new(e) as BoxError)
    } else {
        // TODO: Keep the two bindings' empty-ASCII metadata difference unchanged.
        Ok(binding.handle(request, &*http).await.map(Either::Right))
    }
}

fn connection_result(result: std::result::Result<(), BoxError>) -> Result<()> {
    result.map_err(Error::Connection)
}

// Final connection-error ownership belongs to serve, after notifying its callback.
fn connection_is_peer_error(error: &Error) -> bool {
    let mut source: Option<&(dyn StdError + 'static)> = match error {
        Error::Io(error) => Some(error),
        Error::Connection(error) => Some(&**error),
        Error::Task(_) | Error::InvalidConfig(_) => return false,
    };
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<hyper::Error>() {
            if error.is_user() {
                return false;
            }
            if error.is_timeout() {
                return true;
            }
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            )
        {
            return true;
        }
        source = current.source();
    }
    false
}
