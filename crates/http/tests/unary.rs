//! Every binding scenario uses real TCP, hyper auto servers and both HTTP versions.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    convert::Infallible,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use flate2::{Compression, write::GzEncoder};
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Version};
use http_body_util::{BodyExt, Full};
use hyper::{
    client::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use prost::Message;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use triple_core::{Code, Path, Status};
use triple_http::{
    Binding, CallContext, Config, Encoding, UnaryHandler, is_reserved_header, to_http,
};

const PATH: &str = "/demo.Greeter/SayHello";
const JSON: &[u8] = br#"[{"name":"Alice"}]"#;
const REPLY: &[u8] = br#"{"message":"Hello Alice"}"#;

// The IDL fixture is tests/greeter.proto; prost derives its bare message codec.
#[derive(Clone, PartialEq, Message)]
struct HelloRequest {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct HelloReply {
    #[prost(string, tag = "1")]
    message: String,
}

#[derive(Copy, Clone, Debug)]
enum Wire {
    Http1,
    Http2,
}

impl Wire {
    fn version(self) -> Version {
        match self {
            Self::Http1 => Version::HTTP_11,
            Self::Http2 => Version::HTTP_2,
        }
    }
}

enum Behavior {
    Greet,
    Fail(Code),
    Wait,
    InvalidReply,
}

struct Recorded {
    path: String,
    encoding: Encoding,
    message: Bytes,
    context: CallContext,
}

struct Demo {
    behavior: Behavior,
    seen: Arc<Mutex<Option<Recorded>>>,
    dropped: Arc<AtomicUsize>,
}

struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl UnaryHandler for Demo {
    fn is_registered(&self, path: Path<'_>) -> bool {
        path.as_str() == PATH
    }

    async fn call(
        &self,
        path: Path<'_>,
        encoding: Encoding,
        message: Bytes,
        context: CallContext,
    ) -> Result<Bytes, Status> {
        *self.seen.lock().unwrap() = Some(Recorded {
            path: path.as_str().to_owned(),
            encoding,
            message: message.clone(),
            context,
        });
        match self.behavior {
            Behavior::Fail(code) => return Err(Status::new(code, "business failure")),
            Behavior::Wait => {
                let _guard = DropGuard(self.dropped.clone());
                return std::future::pending().await;
            }
            Behavior::InvalidReply => return Ok(Bytes::from_static(b"invalid JSON")),
            Behavior::Greet => {}
        }
        let name = match encoding {
            Encoding::Json => {
                serde_json::from_slice::<serde_json::Value>(&message).unwrap()["name"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            }
            Encoding::Proto => HelloRequest::decode(message).unwrap().name,
        };
        let reply = format!("Hello {name}");
        Ok(match encoding {
            Encoding::Json => Bytes::from(serde_json::json!({"message": reply}).to_string()),
            Encoding::Proto => Bytes::from(HelloReply { message: reply }.encode_to_vec()),
        })
    }
}

enum Sender {
    Http1(http1::SendRequest<Full<Bytes>>),
    Http2(http2::SendRequest<Full<Bytes>>),
}

struct Peer {
    address: std::net::SocketAddr,
    wire: Wire,
    sender: Sender,
    driver: JoinHandle<()>,
    server: JoinHandle<()>,
    seen: Arc<Mutex<Option<Recorded>>>,
    dropped: Arc<AtomicUsize>,
}

struct Spec {
    path: &'static str,
    method: Method,
    media: Option<&'static str>,
    body: Bytes,
    extra: Vec<(&'static str, HeaderValue)>,
}

impl Spec {
    fn json(body: impl Into<Bytes>) -> Self {
        Self {
            path: PATH,
            method: Method::POST,
            media: Some("application/json"),
            body: body.into(),
            extra: Vec::new(),
        }
    }
    fn header(mut self, name: &'static str, value: &'static str) -> Self {
        self.extra.push((name, HeaderValue::from_static(value)));
        self
    }
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Peer {
    async fn start(wire: Wire, behavior: Behavior, config: Config) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(None));
        let dropped = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(Demo {
            behavior,
            seen: seen.clone(),
            dropped: dropped.clone(),
        });
        let binding = Binding::new(config);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(binding.handle(request, handler.as_ref()).await) }
            });
            // Client drivers are explicitly aborted during fixture teardown.
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
        let socket = TcpStream::connect(address).await.unwrap();
        let (sender, driver) = match wire {
            Wire::Http1 => {
                let (sender, connection) = http1::handshake(TokioIo::new(socket)).await.unwrap();
                (
                    Sender::Http1(sender),
                    tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                )
            }
            Wire::Http2 => {
                let (sender, connection) =
                    http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
                        .await
                        .unwrap();
                (
                    Sender::Http2(sender),
                    tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                )
            }
        };
        Self {
            address,
            wire,
            sender,
            driver,
            server,
            seen,
            dropped,
        }
    }

    async fn send(&mut self, spec: Spec) -> Reply {
        let mut request = Request::builder()
            .method(spec.method)
            .uri(format!("http://{}{}", self.address, spec.path))
            .header("host", self.address.to_string())
            .body(Full::new(spec.body))
            .unwrap();
        if let Some(media) = spec.media {
            request
                .headers_mut()
                .insert("content-type", HeaderValue::from_static(media));
        }
        for (name, value) in spec.extra {
            request.headers_mut().append(name, value);
        }
        let response = match &mut self.sender {
            Sender::Http1(sender) => sender.send_request(request).await.unwrap(),
            Sender::Http2(sender) => sender.send_request(request).await.unwrap(),
        };
        assert_eq!(response.version(), self.wire.version());
        let (parts, body) = response.into_parts();
        Reply {
            status: parts.status,
            headers: parts.headers,
            body: body.collect().await.unwrap().to_bytes(),
        }
    }

    async fn finish(self) {
        drop(self.sender);
        self.driver.abort();
        let _ = self.driver.await;
        self.server.abort();
        let _ = self.server.await;
    }
}

fn check_error(reply: &Reply, status: u16, case: &str) {
    assert_eq!(reply.status.as_u16(), status, "{case}");
    assert_eq!(reply.headers["content-type"], "application/json", "{case}");
    assert_eq!(reply.headers["content-length"], "0", "{case}");
    assert!(reply.body.is_empty(), "{case}");
    assert!(!reply.headers.contains_key("content-encoding"), "{case}");
}

fn check_success(reply: &Reply, encoding: Encoding) {
    assert_eq!(reply.status, StatusCode::OK);
    let media = match encoding {
        Encoding::Json => "application/json",
        Encoding::Proto => "application/proto",
    };
    assert_eq!(reply.headers["content-type"], media);
    assert_eq!(
        reply.headers["content-length"]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        reply.body.len()
    );
    assert!(!reply.headers.contains_key("content-encoding"));
}

fn gzip(body: &[u8]) -> Bytes {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).unwrap();
    Bytes::from(encoder.finish().unwrap())
}

async fn json_unary_success(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    let mut spec = Spec::json(Bytes::from_static(JSON));
    spec.media = Some("application/json; charset=utf-8");
    let reply = peer.send(spec).await;
    check_success(&reply, Encoding::Json);
    assert_eq!(reply.body.as_ref(), REPLY);
    {
        let guard = peer.seen.lock().unwrap();
        let seen = guard.as_ref().unwrap();
        assert_eq!(seen.path, PATH);
        assert_eq!(seen.encoding, Encoding::Json);
        assert_eq!(seen.message.as_ref(), br#"{"name":"Alice"}"#);
        assert!(seen.context.deadline.is_none());
        assert!(seen.context.protocol_version.is_none());
    }
    let raw = br#" { "name": "Alice", "id": 184467440737095516160, "name": "Alice" } "#;
    let body = format!("[{}]", std::str::from_utf8(raw).unwrap());
    check_success(&peer.send(Spec::json(body)).await, Encoding::Json);
    assert_eq!(
        peer.seen.lock().unwrap().as_ref().unwrap().message.as_ref(),
        raw.trim_ascii()
    );
    peer.finish().await;
}

async fn proto_unary_success(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    let bytes = Bytes::from(
        HelloRequest {
            name: "Alice".into(),
        }
        .encode_to_vec(),
    );
    let mut spec = Spec::json(bytes.clone());
    spec.media = Some("application/proto; charset=binary");
    let reply = peer.send(spec).await;
    check_success(&reply, Encoding::Proto);
    assert_eq!(
        HelloReply::decode(reply.body).unwrap().message,
        "Hello Alice"
    );
    {
        let guard = peer.seen.lock().unwrap();
        let seen = guard.as_ref().unwrap();
        assert_eq!(seen.message, bytes);
        assert_eq!(seen.encoding, Encoding::Proto);
    }
    peer.finish().await;
}

async fn unknown_method_and_malformed_path_return_404(wire: Wire) {
    for (case, path) in [
        ("unknown_method", "/demo.Greeter/Unknown"),
        ("service_case", "/demo.greeter/SayHello"),
        ("method_case", "/demo.Greeter/sayHello"),
        ("missing_method", "/demo.Greeter"),
        ("empty_service", "//SayHello"),
        ("empty_method", "/demo.Greeter/"),
        ("extra_segment", "/demo.Greeter/SayHello/extra"),
        ("query", "/demo.Greeter/SayHello?name=Alice"),
        ("empty_query", "/demo.Greeter/SayHello?"),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON));
        spec.path = path;
        check_error(&peer.send(spec).await, 404, case);
        assert!(peer.seen.lock().unwrap().is_none(), "{case}");
        peer.finish().await;
    }
}

async fn non_post_returns_405(wire: Wire) {
    for (case, method) in [
        ("get", Method::GET),
        ("put", Method::PUT),
        ("delete", Method::DELETE),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON));
        spec.method = method;
        check_error(&peer.send(spec).await, 405, case);
        peer.finish().await;
    }
}

async fn unsupported_content_types_return_415(wire: Wire) {
    for (case, media) in [
        ("plain_text", Some("text/plain")),
        ("grpc", Some("application/grpc")),
        ("grpc_proto", Some("application/grpc+proto")),
        ("grpc_json", Some("application/grpc+json")),
        ("java_wrapper", Some("application/triple+wrapper")),
        ("missing_type", None),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON));
        spec.media = media;
        check_error(&peer.send(spec).await, 415, case);
        peer.finish().await;
    }
}

async fn invalid_json_bodies_return_400(wire: Wire) {
    for (case, body) in [
        ("non_json", b"not JSON".as_slice()),
        ("non_array", br#"{"name":"Alice"}"#),
        ("empty_array", b"[]"),
        ("multiple_elements", br#"[{"name":"Alice"},{"name":"Bob"}]"#),
        ("empty_body", b""),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        check_error(
            &peer.send(Spec::json(Bytes::from_static(body))).await,
            400,
            case,
        );
        assert!(peer.seen.lock().unwrap().is_none(), "{case}");
        peer.finish().await;
    }
}

async fn invalid_tri_service_timeout_returns_400(wire: Wire) {
    for (case, timeout, duplicate) in [
        ("zero", "0", false),
        ("negative", "-1", false),
        ("non_numeric", "abc", false),
        ("empty", "", false),
        ("duplicate", "1000", true),
        ("embedded_whitespace", "1 0", false),
        ("overflow", "18446744073709551616", false),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON)).header("tri-service-timeout", timeout);
        if duplicate {
            spec = spec.header("tri-service-timeout", timeout);
        }
        check_error(&peer.send(spec).await, 400, case);
        assert!(peer.seen.lock().unwrap().is_none(), "{case}");
        peer.finish().await;
    }
}

async fn duplicate_single_value_headers_return_400(wire: Wire) {
    for (case, name, value) in [
        ("content_type", "content-type", "application/json"),
        ("content_encoding", "content-encoding", "identity"),
        ("protocol_version", "tri-protocol-version", "1"),
        ("service_version", "tri-service-version", "version"),
        ("service_group", "tri-service-group", "group"),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON)).header(name, value);
        if name != "content-type" {
            spec = spec.header(name, value);
        }
        check_error(&peer.send(spec).await, 400, case);
        peer.finish().await;
    }
}

async fn deadline_returns_408_and_drops_handler(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Wait, Config::default()).await;
    let spec = Spec::json(Bytes::from_static(JSON)).header("tri-service-timeout", "20");
    let reply = tokio::time::timeout(Duration::from_secs(2), peer.send(spec))
        .await
        .unwrap();
    check_error(&reply, 408, "expired_deadline");
    assert_eq!(peer.dropped.load(Ordering::SeqCst), 1);
    assert!(
        peer.seen
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .context
            .deadline
            .unwrap()
            .is_expired_at(std::time::Instant::now())
    );
    peer.finish().await;
}

async fn gzip_request_accepted(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    let reply = peer
        .send(Spec::json(gzip(JSON)).header("content-encoding", "gzip"))
        .await;
    check_success(&reply, Encoding::Json);
    assert_eq!(reply.body.as_ref(), REPLY);
    let bytes = HelloRequest {
        name: "Alice".into(),
    }
    .encode_to_vec();
    let mut spec = Spec::json(gzip(&bytes)).header("content-encoding", "gzip");
    spec.media = Some("application/proto");
    let reply = peer.send(spec).await;
    check_success(&reply, Encoding::Proto);
    assert_eq!(
        HelloReply::decode(reply.body).unwrap().message,
        "Hello Alice"
    );
    peer.finish().await;
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    check_error(
        &peer
            .send(Spec::json("bad gzip").header("content-encoding", "gzip"))
            .await,
        400,
        "malformed_gzip",
    );
    peer.finish().await;
}

async fn unsupported_content_encodings_return_415(wire: Wire) {
    for (case, encoding) in [
        ("brotli", "br"),
        ("zstd", "zstd"),
        ("deflate", "deflate"),
        ("unknown", "compress"),
        ("combined", "gzip, identity"),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        check_error(
            &peer
                .send(Spec::json(Bytes::from_static(JSON)).header("content-encoding", encoding))
                .await,
            415,
            case,
        );
        peer.finish().await;
    }
}

async fn oversize_and_decompression_bomb_return_413(wire: Wire) {
    assert_eq!(Config::default().max_request_bytes, 4 * 1024 * 1024);
    let mut boundary = JSON.to_vec();
    boundary.resize(256, b' ');
    for (body, encoding) in [
        (Bytes::from(boundary.clone()), "identity"),
        (gzip(&boundary), "gzip"),
    ] {
        let mut peer = Peer::start(
            wire,
            Behavior::Greet,
            Config {
                max_request_bytes: 256,
            },
        )
        .await;
        check_success(
            &peer
                .send(Spec::json(body).header("content-encoding", encoding))
                .await,
            Encoding::Json,
        );
        peer.finish().await;
    }
    for (case, body, encoding) in [
        (
            "encoded_identity_oversize",
            Bytes::from(vec![b'x'; 257]),
            "identity",
        ),
        ("encoded_gzip_oversize", Bytes::from(vec![0; 257]), "gzip"),
        ("decompression_bomb", gzip(&vec![b'x'; 64 * 1024]), "gzip"),
    ] {
        let mut peer = Peer::start(
            wire,
            Behavior::Greet,
            Config {
                max_request_bytes: 256,
            },
        )
        .await;
        check_error(
            &peer
                .send(Spec::json(body).header("content-encoding", encoding))
                .await,
            413,
            case,
        );
        assert!(peer.seen.lock().unwrap().is_none(), "{case}");
        peer.finish().await;
    }
}

async fn status_to_http_round_trip(wire: Wire) {
    for (case, code, http) in [
        ("invalid_argument", Code::InvalidArgument, 400),
        ("unauthenticated", Code::Unauthenticated, 401),
        ("permission_denied", Code::PermissionDenied, 403),
        ("unimplemented", Code::Unimplemented, 404),
        ("deadline_exceeded", Code::DeadlineExceeded, 408),
        ("aborted", Code::Aborted, 409),
        ("failed_precondition", Code::FailedPrecondition, 412),
        ("unavailable", Code::Unavailable, 503),
    ] {
        assert_eq!(Code::from_http(to_http(code).as_u16()), code, "{case}");
        let mut peer = Peer::start(wire, Behavior::Fail(code), Config::default()).await;
        check_error(
            &peer.send(Spec::json(Bytes::from_static(JSON))).await,
            http,
            case,
        );
        peer.finish().await;
    }
}

async fn metadata_reaches_handler_and_reserved_names_do_not(wire: Wire) {
    for name in [
        "host",
        "content-type",
        "content-length",
        "content-encoding",
        "accept",
        "accept-encoding",
        "user-agent",
        "tri-protocol-version",
        "tri-service-timeout",
        "tri-service-version",
        "tri-service-group",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        assert!(is_reserved_header(name), "{name}");
    }
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    let mut spec = Spec::json(Bytes::from_static(JSON))
        .header("content-length", "18")
        .header("content-encoding", "identity")
        .header("accept", "application/json")
        .header("accept-encoding", "gzip")
        .header("user-agent", "test")
        .header("tri-protocol-version", "1.0.0")
        .header("tri-service-timeout", "1000")
        .header("tri-service-version", "opaque")
        .header("tri-service-group", "group")
        .header("te", "trailers")
        .header("trailer", "unused")
        .header("proxy-authenticate", "Basic")
        .header("proxy-authorization", "Basic reserved")
        .header("authorization", "Bearer application")
        .header("custom", "first")
        .header("custom", "second")
        .header("custom-bin", "AA==, /w")
        .header("grpc-application", "ordinary")
        .header("grpc-application-bin", "AQ");
    // Connection-specific fields are forbidden on HTTP/2 by its transport grammar.
    if matches!(wire, Wire::Http1) {
        spec = spec
            .header("connection", "keep-alive")
            .header("keep-alive", "timeout=5")
            .header("upgrade", "unused");
    }
    check_success(&peer.send(spec).await, Encoding::Json);
    {
        let guard = peer.seen.lock().unwrap();
        let metadata = &guard.as_ref().unwrap().context.metadata;
        assert_eq!(
            metadata.ascii_values("authorization").collect::<Vec<_>>(),
            ["Bearer application"]
        );
        assert_eq!(
            metadata.ascii_values("custom").collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            metadata.binary_values("custom-bin").collect::<Vec<_>>(),
            [&[0][..], &[255][..]]
        );
        assert_eq!(
            metadata
                .ascii_values("grpc-application")
                .collect::<Vec<_>>(),
            ["ordinary"]
        );
        assert_eq!(
            metadata
                .binary_values("grpc-application-bin")
                .collect::<Vec<_>>(),
            [&[1][..]]
        );
        assert_eq!(metadata.iter().count(), 7);
        for (name, _) in metadata.iter() {
            assert!(!is_reserved_header(name), "{name}");
        }
    }
    peer.finish().await;
}

async fn invalid_metadata_is_dropped_and_recorded(wire: Wire) {
    for (case, name, value) in [
        (
            "invalid_name",
            "invalid!name",
            HeaderValue::from_static("value"),
        ),
        (
            "invalid_ascii",
            "invalid-ascii",
            HeaderValue::from_static("bad\tvalue"),
        ),
        (
            "non_ascii",
            "invalid-high",
            HeaderValue::from_bytes(&[255]).unwrap(),
        ),
        ("empty_ascii", "invalid-empty", HeaderValue::from_static("")),
        (
            "invalid_base64",
            "invalid-bin",
            HeaderValue::from_static("!"),
        ),
        (
            "invalid_joined_base64",
            "invalid-joined-bin",
            HeaderValue::from_static("AA==, !"),
        ),
    ] {
        let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON)).header("healthy", "kept");
        spec.extra.push((name, value));
        check_success(&peer.send(spec).await, Encoding::Json);
        {
            let guard = peer.seen.lock().unwrap();
            let context = &guard.as_ref().unwrap().context;
            assert_eq!(context.metadata.iter().count(), 1, "{case}");
            assert_eq!(
                context.metadata.ascii_values("healthy").collect::<Vec<_>>(),
                ["kept"],
                "{case}"
            );
            assert_eq!(context.discarded_metadata, [name], "{case}");
        }
        peer.finish().await;
    }
}

async fn version_and_group_reach_handler(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    let mut spec = Spec::json(Bytes::from_static(JSON))
        .header("tri-service-version", " any: version ")
        .header("tri-service-group", "group:\topaque");
    spec.extra.push((
        "tri-protocol-version",
        HeaderValue::from_bytes(&[255]).unwrap(),
    ));
    check_success(&peer.send(spec).await, Encoding::Json);
    {
        let guard = peer.seen.lock().unwrap();
        let context = &guard.as_ref().unwrap().context;
        // HTTP/1 parsers strip surrounding field whitespace; the binding preserves
        // the value hyper delivers without imposing a version or group grammar.
        let expected = if matches!(wire, Wire::Http1) {
            "any: version"
        } else {
            " any: version "
        };
        assert_eq!(context.service_version.as_deref(), Some(expected));
        assert_eq!(context.service_group.as_deref(), Some("group:\topaque"));
        assert_eq!(context.protocol_version.as_deref(), Some([255].as_slice()));
    }
    peer.finish().await;
}

async fn http1_keep_alive_serves_two_requests(wire: Wire) {
    let mut peer = Peer::start(wire, Behavior::Greet, Config::default()).await;
    for _ in 0..2 {
        let reply = peer.send(Spec::json(Bytes::from_static(JSON))).await;
        check_success(&reply, Encoding::Json);
        assert_eq!(reply.body.as_ref(), REPLY);
    }
    peer.finish().await;
}

async fn every_error_response_is_application_json(wire: Wire) {
    for code in [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::NotFound,
        Code::AlreadyExists,
        Code::ResourceExhausted,
        Code::OutOfRange,
        Code::Internal,
        Code::DataLoss,
    ] {
        let mut peer = Peer::start(wire, Behavior::Fail(code), Config::default()).await;
        let mut spec = Spec::json(Bytes::from_static(JSON));
        spec.media = Some("application/proto");
        check_error(&peer.send(spec).await, 500, "unmapped_handler_status");
        peer.finish().await;
    }
    let mut peer = Peer::start(wire, Behavior::InvalidReply, Config::default()).await;
    check_error(
        &peer.send(Spec::json(Bytes::from_static(JSON))).await,
        500,
        "invalid_handler_json",
    );
    peer.finish().await;
}

macro_rules! network_scenarios {
    ($( $http1:ident, $http2:ident => $scenario:ident; )+) => { $(
        #[tokio::test] async fn $http1() { $scenario(Wire::Http1).await; }
        #[tokio::test] async fn $http2() { $scenario(Wire::Http2).await; }
    )+ };
}

network_scenarios! {
    json_unary_success_http1, json_unary_success_http2 => json_unary_success;
    proto_unary_success_http1, proto_unary_success_http2 => proto_unary_success;
    unknown_method_and_malformed_path_return_404_http1, unknown_method_and_malformed_path_return_404_http2 => unknown_method_and_malformed_path_return_404;
    non_post_returns_405_http1, non_post_returns_405_http2 => non_post_returns_405;
    unsupported_content_types_return_415_http1, unsupported_content_types_return_415_http2 => unsupported_content_types_return_415;
    invalid_json_bodies_return_400_http1, invalid_json_bodies_return_400_http2 => invalid_json_bodies_return_400;
    invalid_tri_service_timeout_returns_400_http1, invalid_tri_service_timeout_returns_400_http2 => invalid_tri_service_timeout_returns_400;
    duplicate_single_value_headers_return_400_http1, duplicate_single_value_headers_return_400_http2 => duplicate_single_value_headers_return_400;
    deadline_returns_408_and_drops_handler_http1, deadline_returns_408_and_drops_handler_http2 => deadline_returns_408_and_drops_handler;
    gzip_request_accepted_http1, gzip_request_accepted_http2 => gzip_request_accepted;
    unsupported_content_encodings_return_415_http1, unsupported_content_encodings_return_415_http2 => unsupported_content_encodings_return_415;
    oversize_and_decompression_bomb_return_413_http1, oversize_and_decompression_bomb_return_413_http2 => oversize_and_decompression_bomb_return_413;
    status_to_http_round_trip_http1, status_to_http_round_trip_http2 => status_to_http_round_trip;
    metadata_reaches_handler_and_reserved_names_do_not_http1, metadata_reaches_handler_and_reserved_names_do_not_http2 => metadata_reaches_handler_and_reserved_names_do_not;
    invalid_metadata_is_dropped_and_recorded_http1, invalid_metadata_is_dropped_and_recorded_http2 => invalid_metadata_is_dropped_and_recorded;
    version_and_group_reach_handler_http1, version_and_group_reach_handler_http2 => version_and_group_reach_handler;
    http1_keep_alive_serves_two_requests_http1, http1_keep_alive_serves_two_requests_http2 => http1_keep_alive_serves_two_requests;
    every_error_response_is_application_json_http1, every_error_response_is_application_json_http2 => every_error_response_is_application_json;
}
