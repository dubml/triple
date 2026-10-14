//! Replica of Server::serve_io's tracker/stop/StreamExecutor, auto builder,
//! HTTP/1 timer, every HTTP/2 option, graceful_shutdown timeout and final
//! stop.cancel -> tracker.close -> tracker.wait sequence (src/lib.rs:191-218,
//! 226-257). Default settings are copied with shutdown_timeout overridden to 30 ms.
//! The service reproduces dispatch's JSON branch at src/lib.rs:294:
//! Binding::handle -> Either::Right.
//! Only CountingExecutor and RecordingIo add observations. This replica alone
//! cannot prove routing through Server: single_port_http2_json_deadline_returns_408
//! exercises the real server alongside these counters.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::Bytes;
use http_body_util::Either;
use hyper_util::rt::{TokioIo, TokioTimer};
use rpc::{CancellationToken, StreamExecutor};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::task::TaskTracker;
use triple_core::{Path, Status};
use triple_http::{CallContext, Encoding, UnaryHandler};

const WAIT: Duration = Duration::from_secs(2);
const DRAIN: Duration = Duration::from_millis(30);
struct Counts {
    active: AtomicUsize,
    dropped: AtomicUsize,
    finished_ms: Mutex<Vec<u128>>,
    epoch: Instant,
    started: Notify,
}
impl Counts {
    fn new(epoch: Instant) -> Self {
        Self {
            active: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            finished_ms: Mutex::default(),
            epoch,
            started: Notify::new(),
        }
    }
}
struct Guard(Arc<Counts>);
impl Guard {
    fn new(counts: Arc<Counts>) -> Self {
        counts.active.fetch_add(1, Ordering::SeqCst);
        Self(counts)
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.0
            .finished_ms
            .lock()
            .unwrap()
            .push(self.0.epoch.elapsed().as_millis());
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct CountingExecutor(StreamExecutor, Arc<Counts>);
impl<F: Future + Send + 'static> hyper::rt::Executor<F> for CountingExecutor
where
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let guard = Guard::new(self.1.clone());
        self.0.execute(async move {
            let _guard = guard;
            future.await
        });
    }
}
struct RecordingIo {
    io: TcpStream,
    written: Arc<Mutex<Vec<u8>>>,
}
impl AsyncRead for RecordingIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for RecordingIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = result {
            self.written.lock().unwrap().extend_from_slice(&buf[..n]);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
struct JsonWork {
    counts: Arc<Counts>,
    large: bool,
}
impl UnaryHandler for JsonWork {
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
        let _guard = Guard::new(self.counts.clone());
        self.counts.started.notify_one();
        if self.large {
            Ok(Bytes::from(format!("\"{}\"", "x".repeat(1024 * 1024))))
        } else {
            std::future::pending().await
        }
    }
}
async fn serve(
    listener: TcpListener,
    handler: Arc<JsonWork>,
    streams: Arc<Counts>,
    written: Arc<Mutex<Vec<u8>>>,
    shutdown: CancellationToken,
) -> Result<(), triple_server::BoxError> {
    let (io, _) = listener.accept().await?;
    let tracker = TaskTracker::new();
    let stop = CancellationToken::new();
    let mut builder = hyper_util::server::conn::auto::Builder::new(CountingExecutor(
        StreamExecutor::new(tracker.clone(), stop.clone()),
        streams,
    ));
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10));
    let s = rpc::http2_settings(rpc::Limits::default());
    builder
        .http2()
        .timer(TokioTimer::new())
        .initial_stream_window_size(s.initial_stream_window_size)
        .initial_connection_window_size(s.initial_connection_window_size)
        .adaptive_window(false)
        .max_frame_size(s.max_frame_size)
        .max_concurrent_streams(s.max_concurrent_streams)
        .max_header_list_size(u32::try_from(s.max_header_list_size).unwrap())
        .max_send_buf_size(s.max_send_buf_size)
        .max_pending_accept_reset_streams(s.max_pending_accept_reset_streams)
        .max_local_error_reset_streams(s.max_local_error_reset_streams)
        .keep_alive_interval(s.keep_alive_interval)
        .keep_alive_timeout(s.keep_alive_timeout)
        .auto_date_header(false);
    let binding = triple_http::Binding::default();
    let connection = builder.serve_connection(
        TokioIo::new(RecordingIo { io, written }),
        hyper::service::service_fn(move |request| {
            let handler = handler.clone();
            json_request(request, handler, binding)
        }),
    );
    tokio::pin!(connection);
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            match tokio::time::timeout(DRAIN, &mut connection).await { Ok(result) => result, Err(_) => Ok(()) }
        },
        result = &mut connection => result,
    };
    stop.cancel();
    tracker.close();
    tracker.wait().await;
    result
}
async fn json_request(
    request: http::Request<hyper::body::Incoming>,
    handler: Arc<JsonWork>,
    binding: triple_http::Binding,
) -> Result<http::Response<triple_server::ResponseBody>, triple_server::BoxError> {
    Ok(binding.handle(request, &*handler).await.map(Either::Right))
}
fn data_bytes(bytes: &[u8]) -> usize {
    let mut total = 0;
    let mut pos = 0;
    while pos + 9 <= bytes.len() {
        let len = (usize::from(bytes[pos]) << 16)
            | (usize::from(bytes[pos + 1]) << 8)
            | usize::from(bytes[pos + 2]);
        if pos + 9 + len > bytes.len() {
            break;
        }
        if bytes[pos + 3] == 0 {
            total += len;
        }
        pos += 9 + len;
    }
    total
}
#[tokio::test]
async fn single_port_http2_json_deadline_drops_handler() {
    observe(false, true).await;
}
#[tokio::test]
async fn json_response_larger_than_window_shutdown_is_bounded() {
    observe(true, false).await;
}
#[tokio::test]
async fn json_response_larger_than_window_with_deadline_is_observed() {
    observe(true, true).await;
}
async fn observe(large: bool, deadline: bool) {
    let epoch = Instant::now();
    let handlers = Arc::new(Counts::new(epoch));
    let streams = Arc::new(Counts::new(epoch));
    let written = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(
        listener,
        Arc::new(JsonWork {
            counts: handlers.clone(),
            large,
        }),
        streams.clone(),
        written.clone(),
        stop.clone(),
    ));
    let mut client = transport::Client::connect(&address, rpc::Limits::default())
        .await
        .unwrap();
    client.ready().await.unwrap();
    let mut request = http::Request::builder()
        .method("POST")
        .uri(format!("http://{address}/demo.Greeter/SayHello"))
        .header("content-type", "application/json");
    if deadline {
        request = request.header("tri-service-timeout", "300");
    }
    let expiry = tokio::time::Instant::now() + Duration::from_millis(300);
    let (response, mut request_body) = client
        .send_request(request.body(()).unwrap(), false)
        .unwrap();
    request_body
        .send_data(Bytes::from_static(b"[{}]"), true)
        .unwrap();
    tokio::time::timeout(WAIT, handlers.started.notified())
        .await
        .unwrap();
    if large {
        // Never poll the response future/body or release its 65535-byte window.
        tokio::time::sleep_until(expiry + WAIT).await;
        let sent = data_bytes(&written.lock().unwrap());
        println!(
            "JSON large=true timeout={deadline}: handler_finished_ms={:?}, stream_finished_ms={:?}, handler_active={}, stream_active={}, DATA_written={sent}, unsent={}",
            handlers.finished_ms.lock().unwrap(),
            streams.finished_ms.lock().unwrap(),
            handlers.active.load(Ordering::SeqCst),
            streams.active.load(Ordering::SeqCst),
            (1024usize * 1024 + 2).saturating_sub(sent)
        );
    } else {
        tokio::time::timeout_at(expiry + WAIT, async {
            while handlers.active.load(Ordering::SeqCst) != 0
                || streams.active.load(Ordering::SeqCst) != 0
                || handlers.dropped.load(Ordering::SeqCst) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let head = tokio::time::timeout(WAIT, response).await.unwrap().unwrap();
        assert_eq!(head.status(), 408);
        println!(
            "JSON pending: status=408, handler_finished_ms={:?}, stream_finished_ms={:?}, handler_active=0, stream_active=0",
            handlers.finished_ms.lock().unwrap(),
            streams.finished_ms.lock().unwrap()
        );
    }
    stop.cancel();
    tokio::time::timeout(DRAIN + WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            handlers.active.load(Ordering::SeqCst),
            streams.active.load(Ordering::SeqCst)
        ),
        (0, 0)
    );
    drop(request_body);
    drop(client);
}
