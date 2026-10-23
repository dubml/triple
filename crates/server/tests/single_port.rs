//! Real single-port HTTP/1, HTTP/2, TLS, shutdown and flow-control tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[path = "../fake/common/mod.rs"]
mod demo;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use protos::echo::{EchoMessage, echo_client::EchoClient};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};
use triple_core::{Path, Status};
use triple_http::{CallContext, Encoding, UnaryHandler};
use triple_rpc::{CancellationToken, Incoming, Outgoing, RpcKind};
use triple_server::{Config, Server};

const WAIT: Duration = Duration::from_secs(2);
async fn start<S: triple_rpc::Service + 'static, H: UnaryHandler + 'static>(
    rpc: Arc<S>,
    http: Arc<H>,
    config: Config,
) -> (
    String,
    CancellationToken,
    JoinHandle<triple_server::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let server = Arc::new(Server::new(rpc, http, config));
    (
        address,
        shutdown,
        tokio::spawn(server.serve(listener, stop)),
    )
}
async fn finish(stop: CancellationToken, server: JoinHandle<triple_server::Result<()>>) {
    stop.cancel();
    tokio::time::timeout(WAIT, server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
fn request(media: &str, body: Bytes) -> hyper::Request<Full<Bytes>> {
    hyper::Request::builder()
        .method("POST")
        .uri("/demo.Greeter/SayHello")
        .header("content-type", media)
        .body(Full::new(body))
        .unwrap()
}
async fn http<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    io: T,
    h2: bool,
    req: hyper::Request<Full<Bytes>>,
) -> (http::StatusCode, http::HeaderMap, Bytes) {
    if h2 {
        let (mut sender, connection) =
            hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .handshake(TokioIo::new(io))
                .await
                .unwrap();
        let driver = tokio::spawn(connection);
        let response = sender.send_request(req).await.unwrap();
        let (head, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        drop(sender);
        driver.abort();
        let _ = driver.await;
        (head.status, head.headers, bytes)
    } else {
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let response = sender.send_request(req).await.unwrap();
        let (head, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        drop(sender);
        driver.abort();
        let _ = driver.await;
        (head.status, head.headers, bytes)
    }
}
fn message(media: &str) -> Bytes {
    if media.starts_with("application/json") {
        Bytes::from_static(br#"[{"name":"Alice"}]"#)
    } else {
        Bytes::from(
            demo::HelloRequest {
                name: "Alice".into(),
            }
            .encode_to_vec(),
        )
    }
}
fn check(media: &str, status: http::StatusCode, headers: http::HeaderMap, body: Bytes) {
    assert_eq!(status, 200);
    if media.starts_with("application/json") {
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"message":"Hello Alice"})
        );
    } else {
        assert_eq!(headers["content-type"], "application/proto");
        assert_eq!(
            demo::HelloReply::decode(body).unwrap().message,
            "Hello Alice"
        );
    }
}

#[tokio::test]
async fn single_port_tonic_unary_succeeds() {
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    let mut client = EchoClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    assert_eq!(
        client
            .unary(EchoMessage {
                text: "tonic".into()
            })
            .await
            .unwrap()
            .into_inner()
            .text,
        "tonic"
    );
    drop(client);
    finish(stop, server).await;
}
#[tokio::test]
async fn single_port_http1_and_http2_json_and_proto_succeed() {
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    for h2 in [false, true] {
        for media in ["application/json; charset=utf-8", "application/proto"] {
            let (s, h, b) = http(
                TcpStream::connect(&address).await.unwrap(),
                h2,
                request(media, message(media)),
            )
            .await;
            check(media, s, h, b);
        }
    }
    finish(stop, server).await;
}
#[tokio::test]
async fn single_port_http1_grpc_returns_505_and_other_media_uses_http_binding() {
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    for media in [
        "application/grpc",
        "application/grpc+proto",
        "application/grpc+json",
        "application/triple+wrapper",
    ] {
        let (s, h, b) = http(
            TcpStream::connect(&address).await.unwrap(),
            false,
            request(media, Bytes::new()),
        )
        .await;
        assert_eq!(s, 505);
        assert_eq!(h["content-type"], "application/json");
        assert!(b.is_empty());
    }
    for media in [
        "text/plain",
        "application/grpc+json",
        "application/triple+wrapper",
    ] {
        let (s, _, b) = http(
            TcpStream::connect(&address).await.unwrap(),
            true,
            request(media, Bytes::new()),
        )
        .await;
        assert_eq!(s, 415);
        assert!(b.is_empty());
    }
    finish(stop, server).await;
}

#[tokio::test]
async fn same_http2_connection_serves_grpc_then_json() {
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    // Use one hyper HTTP/2 sender for both wire formats.
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(TcpStream::connect(&address).await.unwrap()))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let payload = EchoMessage {
        text: "same".into(),
    }
    .encode_to_vec();
    let mut frame = vec![0];
    frame.extend(u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend(payload);
    let req = hyper::Request::builder()
        .method("POST")
        .uri("http://localhost/test.Echo/Unary")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(Full::new(Bytes::from(frame)))
        .unwrap();
    let mut response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), 200);
    let mut data = Vec::new();
    let mut trailers = None;
    while let Some(frame) = response.frame().await {
        let frame = frame.unwrap();
        match frame.into_data() {
            Ok(bytes) => data.extend_from_slice(&bytes),
            Err(frame) => trailers = frame.into_trailers().ok(),
        }
    }
    assert_eq!(EchoMessage::decode(&data[5..]).unwrap().text, "same");
    assert_eq!(trailers.unwrap()["grpc-status"], "0");
    let response = sender
        .send_request(request("application/json", message("application/json")))
        .await
        .unwrap();
    let (head, body) = response.into_parts();
    check(
        "application/json",
        head.status,
        head.headers,
        body.collect().await.unwrap().to_bytes(),
    );
    drop(sender);
    driver.abort();
    let _ = driver.await;
    finish(stop, server).await;
}

fn tls_configs() -> (
    Arc<triple_rpc::rustls::ServerConfig>,
    triple_rpc::rustls::RootCertStore,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let der = cert.cert.der().clone();
    let mut roots = triple_rpc::rustls::RootCertStore::empty();
    roots.add(der.clone()).unwrap();
    let identity = triple_rpc::Identity {
        certificates: vec![der],
        private_key: triple_rpc::rustls::pki_types::PrivatePkcs8KeyDer::from(
            cert.signing_key.serialize_der(),
        )
        .into(),
    };
    (
        triple_rpc::server_tls_config(identity, None).unwrap(),
        roots,
    )
}
async fn tls_io(
    address: &str,
    roots: triple_rpc::rustls::RootCertStore,
    h2: bool,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut config = (*triple_rpc::client_tls_config(roots, None).unwrap()).clone();
    config.alpn_protocols = vec![if h2 {
        b"h2".to_vec()
    } else {
        b"http/1.1".to_vec()
    }];
    let io = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(
            triple_rpc::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        io.get_ref().1.alpn_protocol(),
        Some(if h2 {
            b"h2".as_slice()
        } else {
            b"http/1.1".as_slice()
        })
    );
    io
}
#[tokio::test]
async fn single_port_tls_h2_grpc_json_and_http1_json_and_505_succeed() {
    let (tls, roots) = tls_configs();
    let config = Config {
        tls: Some(tls),
        ..Config::default()
    };
    let (address, stop, server) =
        start(Arc::new(demo::Echo), Arc::new(demo::Greeter), config).await;
    let mut client = triple_rpc::Client::connect_tls(
        &address,
        "localhost",
        triple_rpc::client_tls_config(roots.clone(), None).unwrap(),
        triple_rpc::Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .unary::<_, EchoMessage>(
                "/test.Echo/Unary",
                &EchoMessage { text: "TLS".into() },
                &triple_rpc::Metadata::new()
            )
            .await
            .unwrap()
            .text,
        "TLS"
    );
    drop(client);
    for h2 in [true, false] {
        let (s, h, b) = http(
            tls_io(&address, roots.clone(), h2).await,
            h2,
            request("application/json", message("application/json")),
        )
        .await;
        check("application/json", s, h, b);
    }
    let (s, h, b) = http(
        tls_io(&address, roots, false).await,
        false,
        request("application/grpc", Bytes::new()),
    )
    .await;
    assert_eq!(s, 505);
    assert_eq!(h["content-type"], "application/json");
    assert!(b.is_empty());
    finish(stop, server).await;
}

#[tokio::test]
async fn http1_slow_headers_connection_is_closed() {
    let observed = Arc::new(AtomicUsize::new(0));
    let errors = observed.clone();
    let config = Config {
        header_read_timeout: Duration::from_millis(200),
        on_connection_error: Some(Arc::new(move |error| {
            if let triple_server::Error::Connection(error) = error
                && error
                    .downcast_ref::<hyper::Error>()
                    .is_some_and(hyper::Error::is_timeout)
            {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        })),
        ..Config::default()
    };
    let (address, stop, server) =
        start(Arc::new(demo::Echo), Arc::new(demo::Greeter), config).await;
    let mut io = TcpStream::connect(&address).await.unwrap();
    io.write_all(b"POST /demo.Greeter/").await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(WAIT, io.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let (s, h, b) = http(
        TcpStream::connect(&address).await.unwrap(),
        false,
        request("application/json", message("application/json")),
    )
    .await;
    check("application/json", s, h, b);
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    finish(stop, server).await;
}

async fn settings(address: &str) -> Vec<u8> {
    let mut io = TcpStream::connect(address).await.unwrap();
    io.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    io.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
    let mut h = [0; 9];
    io.read_exact(&mut h).await.unwrap();
    assert_eq!(h[3], 4);
    let len = (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
    let mut p = vec![0; len];
    io.read_exact(&mut p).await.unwrap();
    p
}
fn effective(p: &[u8]) -> [Option<u32>; 5] {
    assert_eq!(p.len() % 6, 0);
    let mut s = [Some(4096), Some(16384), Some(65535), None, None];
    for item in p.as_chunks::<6>().0 {
        let id = u16::from_be_bytes([item[0], item[1]]);
        let value = u32::from_be_bytes([item[2], item[3], item[4], item[5]]);
        match id {
            1 => s[0] = Some(value),
            5 => s[1] = Some(value),
            4 => s[2] = Some(value),
            3 => s[3] = Some(value),
            6 => s[4] = Some(value),
            _ => {}
        }
    }
    s
}
#[tokio::test]
async fn single_port_http2_settings_match_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let native = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        triple_rpc::serve_connection(
            io,
            Arc::new(demo::Echo),
            triple_rpc::Limits::default(),
            shutdown,
            Duration::from_millis(20),
        )
        .await
    });
    let rpc = settings(&address).await;
    println!("rpc SETTINGS: {rpc:02x?}");
    stop.cancel();
    native.await.unwrap().unwrap();
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    let mixed = settings(&address).await;
    println!("single-port SETTINGS: {mixed:02x?}");
    assert_eq!(effective(&rpc), effective(&mixed));
    assert_eq!(
        effective(&mixed),
        [Some(4096), Some(16384), Some(65535), Some(128), Some(8192)]
    );
    assert!(
        !mixed
            .as_chunks::<6>()
            .0
            .iter()
            .any(|v| v[0] == 0 && v[1] == 1)
    );
    assert!(
        triple_rpc::http2_settings(triple_rpc::Limits::default())
            .keep_alive_interval
            .is_none()
    );
    finish(stop, server).await;
}

#[derive(Default)]
struct Counts {
    started: Notify,
    active: AtomicUsize,
    dropped: AtomicUsize,
}
struct Guard(Arc<Counts>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct RpcWork {
    counts: Arc<Counts>,
    flood: bool,
    delay: Duration,
}
impl triple_rpc::Service for RpcWork {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::ServerStreaming)
    }
    async fn call(
        &self,
        _: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> triple_rpc::Result<()> {
        self.counts.active.fetch_add(1, Ordering::SeqCst);
        let _guard = Guard(self.counts.clone());
        request.message::<EchoMessage>().await?;
        self.counts.started.notify_one();
        if self.flood {
            loop {
                response
                    .send(&EchoMessage {
                        text: "x".repeat(32 * 1024),
                    })
                    .await?;
            }
        } else {
            tokio::time::sleep(self.delay).await;
            response
                .send(&EchoMessage {
                    text: "finished".into(),
                })
                .await
        }
    }
}
struct HttpWork {
    counts: Arc<Counts>,
    delay: Duration,
}
impl UnaryHandler for HttpWork {
    fn is_registered(&self, _: Path<'_>) -> bool {
        true
    }
    async fn call(
        &self,
        _: Path<'_>,
        _: Encoding,
        _: Bytes,
        _: CallContext,
    ) -> Result<Bytes, Status> {
        self.counts.active.fetch_add(1, Ordering::SeqCst);
        let _guard = Guard(self.counts.clone());
        self.counts.started.notify_one();
        tokio::time::sleep(self.delay).await;
        Ok(Bytes::from_static(b"{}"))
    }
}

#[tokio::test]
async fn single_port_stalled_peer_deadline_resets_stream() {
    let counts = Arc::new(Counts::default());
    let (address, stop, server) = start(
        Arc::new(RpcWork {
            counts: counts.clone(),
            flood: true,
            delay: Duration::ZERO,
        }),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    // Match flow_control: server deadline only, with no local reader deadline.
    let limits = triple_rpc::Limits::default();
    let mut client = triple_transport::Client::connect(&address, limits)
        .await
        .unwrap();
    client.ready().await.unwrap();
    let expiry = tokio::time::Instant::now() + Duration::from_millis(300);
    let deadline = triple_rpc::Deadline::after(Duration::from_millis(300)).unwrap();
    let req = triple_rpc::prepare_request(
        &format!("http://{address}").parse().unwrap(),
        "/test.Flow/Stall",
        &triple_rpc::Metadata::new(),
        Some(&deadline.to_header().unwrap()),
        limits,
    )
    .unwrap();
    let (response, stream) = client.send_request(req, false).unwrap();
    let mut requests = Outgoing::new(stream, limits, true, triple_rpc::SendRole::Request);
    requests
        .send(&EchoMessage {
            text: String::new(),
        })
        .await
        .unwrap();
    requests.close().unwrap();
    let head = triple_rpc::parse_response(response.await.unwrap(), limits.max_header_size).unwrap();
    assert!(head.status.is_none());
    let mut incoming = Incoming::new(head.body, head.metadata, limits, false, true);
    assert!(incoming.deadline().is_none());
    tokio::time::timeout(WAIT, counts.started.notified())
        .await
        .unwrap();
    tokio::time::timeout_at(expiry + WAIT, async {
        while counts.active.load(Ordering::SeqCst) != 0
            || counts.dropped.load(Ordering::SeqCst) == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    let mut messages = 0;
    let reason = tokio::time::timeout(WAIT, async {
        loop {
            match incoming.message::<EchoMessage>().await {
                Ok(Some(message)) => {
                    assert_eq!(message.text.len(), 32 * 1024);
                    messages += 1;
                }
                Err(triple_rpc::Error::Transport(triple_transport::Error::Transport(error))) => {
                    assert!(error.is_reset());
                    println!("single-port stalled: messages={messages}, {error:?}");
                    return u32::from(error.reason().unwrap());
                }
                other => panic!("expected remote CANCEL reset, got {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reason, 8, "RST_STREAM(CANCEL)");
    assert_eq!(messages, 1);
    stop.cancel();
    tokio::time::timeout(WAIT, server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(incoming);
    drop(requests);
    drop(client);
}

#[tokio::test]
async fn single_port_shutdown_joins_stream_tasks() {
    shutdown_calls(Duration::from_secs(60), Duration::from_millis(30), false).await;
}
#[tokio::test]
async fn single_port_graceful_shutdown_finishes_grpc_and_json() {
    shutdown_calls(Duration::from_millis(60), Duration::from_secs(1), true).await;
}

#[tokio::test]
async fn single_port_http2_json_deadline_returns_408() {
    let counts = Arc::new(Counts::default());
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(NeverJson(counts.clone())),
        Config::default(),
    )
    .await;
    let mut req = request("application/json", Bytes::from_static(b"[{}]"));
    req.headers_mut()
        .insert("tri-service-timeout", "300".parse().unwrap());
    let (status, _, _) = tokio::time::timeout(
        Duration::from_millis(300) + WAIT,
        http(TcpStream::connect(&address).await.unwrap(), true, req),
    )
    .await
    .unwrap();
    assert_eq!(status, 408);
    assert_eq!(counts.active.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    finish(stop, server).await;
}
struct NeverJson(Arc<Counts>);
impl UnaryHandler for NeverJson {
    fn is_registered(&self, _: Path<'_>) -> bool {
        true
    }
    async fn call(
        &self,
        _: Path<'_>,
        _: Encoding,
        _: Bytes,
        _: CallContext,
    ) -> Result<Bytes, Status> {
        self.0.active.fetch_add(1, Ordering::SeqCst);
        let _guard = Guard(self.0.clone());
        std::future::pending().await
    }
}

#[tokio::test]
async fn single_port_date_header_policy() {
    let (address, stop, server) = start(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Config::default(),
    )
    .await;
    for h2 in [false, true] {
        let (s, h, b) = http(
            TcpStream::connect(&address).await.unwrap(),
            h2,
            request("application/json", message("application/json")),
        )
        .await;
        assert_eq!(h.contains_key("date"), !h2);
        check("application/json", s, h, b);
    }
    let mut client = EchoClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let response = client
        .unary(EchoMessage {
            text: "date".into(),
        })
        .await
        .unwrap();
    assert!(response.metadata().get("date").is_none());
    assert_eq!(response.into_inner().text, "date");
    drop(client);
    finish(stop, server).await;
}
async fn shutdown_calls(delay: Duration, timeout: Duration, success: bool) {
    let grpc = Arc::new(Counts::default());
    let json = Arc::new(Counts::default());
    let config = Config {
        shutdown_timeout: timeout,
        ..Config::default()
    };
    let (address, stop, server) = start(
        Arc::new(RpcWork {
            counts: grpc.clone(),
            flood: false,
            delay,
        }),
        Arc::new(HttpWork {
            counts: json.clone(),
            delay,
        }),
        config,
    )
    .await;
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(TcpStream::connect(&address).await.unwrap()))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let req = hyper::Request::builder()
        .method("POST")
        .uri("http://localhost/test.Work/Wait")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])))
        .unwrap();
    let mut grpc_response = sender.send_request(req).await.unwrap();
    grpc.started.notified().await;
    let mut json_sender = sender.clone();
    let call = tokio::spawn(async move {
        json_sender
            .send_request(request("application/json", Bytes::from_static(b"[{}]")))
            .await
    });
    json.started.notified().await;
    stop.cancel();
    tokio::time::timeout(WAIT, server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(grpc.active.load(Ordering::SeqCst), 0);
    assert_eq!(json.active.load(Ordering::SeqCst), 0);
    assert_eq!(grpc.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(json.dropped.load(Ordering::SeqCst), 1);
    if success {
        let mut data = Vec::new();
        let mut trailers = None;
        while let Some(frame) = grpc_response.frame().await {
            match frame.unwrap().into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => trailers = frame.into_trailers().ok(),
            }
        }
        assert_eq!(EchoMessage::decode(&data[5..]).unwrap().text, "finished");
        assert_eq!(trailers.unwrap()["grpc-status"], "0");
        let json_response = call.await.unwrap().unwrap();
        assert_eq!(json_response.status(), 200);
        assert_eq!(
            json_response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
            "{}"
        );
    } else {
        assert!(call.await.unwrap().is_err());
    }
    drop(grpc_response);
    drop(sender);
    driver.abort();
    let _ = driver.await;
}
