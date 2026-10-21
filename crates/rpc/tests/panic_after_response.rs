//! Partial responses and queued DATA survive a handler panic without losing trailers.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::Bytes;
use http_body::{Body, Frame};
use prost::Message;
use std::{
    io::{self, IoSlice},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_util::task::TaskTracker;
use triple_rpc::{
    CancellationToken, HandlerPanicPhase, Incoming, Limits, Outgoing, RpcKind, Service,
};

const WAIT: Duration = Duration::from_secs(2);
const PATH: &str = "/test.Panic/Partial";
const WINDOW: usize = 65_535;

#[derive(Clone, PartialEq, Message)]
struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct WireFrame {
    kind: u8,
    flags: u8,
    stream: u32,
    length: usize,
}

#[derive(Default)]
struct Wire {
    pending: Vec<u8>,
    frames: Vec<WireFrame>,
}

#[derive(Debug)]
struct PanicEvent {
    path: String,
    phase: HandlerPanicPhase,
    sent: usize,
    body_data: usize,
    wire_data: usize,
}

#[derive(Default)]
struct Counts {
    sent: AtomicUsize,
    body_data: AtomicUsize,
    body_trailers: AtomicUsize,
    wire_data: AtomicUsize,
    dropped: AtomicBool,
    panics: Mutex<Vec<PanicEvent>>,
    wire: Mutex<Wire>,
    tracker: TaskTracker,
}

impl Counts {
    fn written(&self, bytes: &[u8]) {
        let mut wire = self.wire.lock().unwrap();
        wire.pending.extend_from_slice(bytes);
        while wire.pending.len() >= 9 {
            let length = (usize::from(wire.pending[0]) << 16)
                | (usize::from(wire.pending[1]) << 8)
                | usize::from(wire.pending[2]);
            if wire.pending.len() < 9 + length {
                break;
            }
            let frame = WireFrame {
                kind: wire.pending[3],
                flags: wire.pending[4],
                stream: u32::from_be_bytes(wire.pending[5..9].try_into().unwrap()) & 0x7fff_ffff,
                length,
            };
            if frame.kind == 0 && frame.stream == 1 {
                self.wire_data.fetch_add(frame.length, SeqCst);
            }
            wire.frames.push(frame);
            wire.pending.drain(..9 + length);
        }
    }
}

struct RecordedIo(TcpStream, Arc<Counts>);

impl AsyncRead for RecordedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for RecordedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.0).poll_write(cx, bytes);
        if let Poll::Ready(Ok(written)) = result {
            self.1.written(&bytes[..written]);
        }
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.0).poll_write_vectored(cx, buffers);
        if let Poll::Ready(Ok(mut written)) = result {
            for buffer in buffers {
                let take = written.min(buffer.len());
                self.1.written(&buffer[..take]);
                written -= take;
                if written == 0 {
                    break;
                }
            }
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

struct ObservedBody(triple_rpc::ResponseBody, Arc<Counts>);

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = h2::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, h2::Error>>> {
        let result = Pin::new(&mut self.0).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if frame.data_ref().is_some() {
                self.1.body_data.fetch_add(1, SeqCst);
            }
            if frame.trailers_ref().is_some() {
                self.1.body_trailers.fetch_add(1, SeqCst);
            }
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

struct HandlerGuard(Arc<Counts>);
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.dropped.store(true, SeqCst);
    }
}

struct Partial {
    counts: Arc<Counts>,
    messages: Vec<Payload>,
    queued: bool,
}

impl Service for Partial {
    fn contains(&self, path: &str) -> bool {
        path == PATH
    }

    fn kind(&self, path: &str) -> Option<RpcKind> {
        self.contains(path).then_some(RpcKind::Bidirectional)
    }

    fn on_handler_panic(&self, path: &str, phase: HandlerPanicPhase) {
        self.counts.panics.lock().unwrap().push(PanicEvent {
            path: path.to_owned(),
            phase,
            sent: self.counts.sent.load(SeqCst),
            body_data: self.counts.body_data.load(SeqCst),
            wire_data: self.counts.wire_data.load(SeqCst),
        });
    }

    async fn call(
        &self,
        _: &str,
        _: &mut Incoming,
        response: &mut Outgoing,
    ) -> triple_rpc::Result<()> {
        let _guard = HandlerGuard(self.counts.clone());
        response.send(&self.messages[0]).await?;
        self.counts.sent.fetch_add(1, SeqCst);
        if self.queued {
            wait_for(|| self.counts.wire_data.load(SeqCst) == WINDOW).await;
        }
        response.send(&self.messages[1]).await?;
        self.counts.sent.fetch_add(1, SeqCst);
        panic!("intentional panic after two completed sends");
    }
}

async fn wait_for(predicate: impl Fn() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// The queued case mirrors serve_tracked_connection's Builder, StreamExecutor,
// handle and stop/close/wait sequence. Its only body change delegates every poll
// unchanged while counting DATA taken from the private channel. The ordinary
// case below also exercises the real public serve_connection entry point.
// Protocol detection/pre-cancel branches are not reproduced: the test completes
// HTTP/2 setup before issuing its only request.
async fn observed_connection(
    io: RecordedIo,
    service: Arc<Partial>,
    shutdown: CancellationToken,
    counts: Arc<Counts>,
) -> triple_rpc::Result<()> {
    let stop = CancellationToken::new();
    let tracker = counts.tracker.clone();
    let executor = triple_rpc::StreamExecutor::new(tracker.clone(), stop.clone());
    let mut builder = hyper::server::conn::http2::Builder::new(executor);
    let s = triple_rpc::http2_settings(Limits::default());
    builder
        .max_header_list_size(u32::try_from(s.max_header_list_size).unwrap())
        .max_concurrent_streams(s.max_concurrent_streams)
        .max_frame_size(s.max_frame_size)
        .initial_stream_window_size(s.initial_stream_window_size)
        .initial_connection_window_size(s.initial_connection_window_size)
        .adaptive_window(false)
        .header_table_size(s.header_table_size)
        .keep_alive_interval(s.keep_alive_interval)
        .keep_alive_timeout(s.keep_alive_timeout)
        .max_send_buf_size(s.max_send_buf_size)
        .auto_date_header(false)
        .max_pending_accept_reset_streams(s.max_pending_accept_reset_streams)
        .max_local_error_reset_streams(s.max_local_error_reset_streams);
    let calls = tracker.clone();
    let connection = builder.serve_connection(
        hyper_util::rt::TokioIo::new(io),
        hyper::service::service_fn(move |request| {
            let service = service.clone();
            let calls = calls.clone();
            let counts = counts.clone();
            async move {
                triple_rpc::handle(request, service, Limits::default(), calls)
                    .await
                    .map(|response| response.map(|body| ObservedBody(body, counts)))
            }
        }),
    );
    tokio::pin!(connection);
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            match tokio::time::timeout(Duration::from_millis(100), &mut connection).await {
                Ok(result) => result,
                Err(_) => Ok(()),
            }
        }
        result = &mut connection => result,
    };
    stop.cancel();
    tracker.close();
    tracker.wait().await;
    result.map_err(|error| triple_rpc::Error::Io(io::Error::other(error)))
}

async fn run_case(queued: bool) {
    let counts = Arc::new(Counts::default());
    let messages = if queued {
        (*b"AB")
            .map(|byte| Payload {
                bytes: vec![byte; 256 * 1024],
            })
            .to_vec()
    } else {
        [b"first".as_slice(), b"second".as_slice()]
            .map(|bytes| Payload {
                bytes: bytes.to_vec(),
            })
            .to_vec()
    };
    let service = Arc::new(Partial {
        counts: counts.clone(),
        messages: messages.clone(),
        queued,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let io = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn({
        let counts = counts.clone();
        let stop = stop.clone();
        async move {
            let io = RecordedIo(accepted, counts.clone());
            if queued {
                observed_connection(io, service, stop, counts).await
            } else {
                triple_rpc::serve_connection(
                    io,
                    service,
                    Limits::default(),
                    stop,
                    Duration::from_millis(100),
                )
                .await
            }
        }
    });
    let (mut client, connection) = h2::client::handshake(io).await.unwrap();
    let driver = tokio::spawn(connection);
    let request = triple_rpc::prepare_request(
        &"http://localhost".parse().unwrap(),
        PATH,
        &triple_rpc::Metadata::new(),
        None,
        Limits::default(),
    )
    .unwrap();
    let (response, _request_body) = client.send_request(request, true).unwrap();
    let response = response.await.unwrap();
    assert!(!response.headers().contains_key("grpc-status"));
    let mut body = response.into_body();
    // Do not pull DATA or release any receive-window capacity until the panic.
    wait_for(|| counts.dropped.load(SeqCst) && counts.panics.lock().unwrap().len() == 1).await;
    if queued {
        // One remaining stream task proves the handler task, including finish_call,
        // completed while the client still withheld all DATA/window consumption.
        wait_for(|| counts.tracker.len() == 1).await;
        println!(
            "panic_snapshot={:?}; after_finish body_polled={} wire_data={}",
            counts.panics.lock().unwrap(),
            counts.body_data.load(SeqCst),
            counts.wire_data.load(SeqCst)
        );
        assert_eq!(counts.sent.load(SeqCst), 2);
        // Hyper may prefetch the queued message after finish_call. Freeze the
        // callback's synchronous panic-time snapshot, not a later body poll.
        {
            let events = counts.panics.lock().unwrap();
            assert_eq!(events[0].sent, 2);
            assert_eq!(events[0].body_data, 1);
            assert_eq!(events[0].sent - events[0].body_data, 1);
        }
        assert_eq!(counts.body_trailers.load(SeqCst), 0);
        assert_eq!(counts.wire_data.load(SeqCst), WINDOW);
        println!(
            "queued at panic: completed=2 body_polled=1 channel_queued=1 wire_data=65535 handler_task_finished=true"
        );
    }
    let mut data = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        data.extend_from_slice(&chunk);
        body.flow_control().release_capacity(chunk.len()).unwrap();
    }
    let trailers = body.trailers().await.unwrap().unwrap();
    assert_eq!(trailers["grpc-status"], "13");
    assert_eq!(trailers["grpc-message"], "handler panicked");
    let mut decoder = triple_codec::FrameDecoder::new(Limits::default().max_message_size);
    let mut data = Bytes::from(data);
    let mut received = Vec::new();
    while let Some(message) = decoder.decode(&mut data).unwrap() {
        received.push(Payload::decode(message).unwrap());
    }
    decoder.finish().unwrap();
    assert!(data.is_empty());
    assert_eq!(received, messages);
    stop.cancel();
    server.await.unwrap().unwrap();
    drop(client);
    driver.await.unwrap().unwrap();
    let events = counts.panics.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].path, PATH);
    assert_eq!(events[0].phase, HandlerPanicPhase::Future);
    assert_eq!(events[0].sent, 2);
    if queued {
        assert_eq!(events[0].body_data, 1);
        assert_eq!(events[0].wire_data, WINDOW);
        assert_eq!(counts.body_data.load(SeqCst), 2);
        assert_eq!(counts.body_trailers.load(SeqCst), 1);
        assert!(counts.tracker.is_empty());
    }
    let wire = counts.wire.lock().unwrap();
    let frames: Vec<_> = wire
        .frames
        .iter()
        .filter(|frame| frame.stream == 1)
        .collect();
    assert_eq!(frames.first().unwrap().kind, 1);
    assert_eq!(frames.first().unwrap().flags, 4);
    assert_eq!(frames.last().unwrap().kind, 1);
    assert_eq!(frames.last().unwrap().flags, 5);
    assert!(
        frames[1..frames.len() - 1]
            .iter()
            .all(|f| f.kind == 0 && f.flags == 0)
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| f.kind == 1 && f.flags & 1 != 0)
            .count(),
        1
    );
    for (index, frame) in frames.iter().enumerate() {
        let kind = if frame.kind == 1 { "HEADERS" } else { "DATA" };
        println!(
            "queued={queued} frame={index} type={kind} stream={} flags=0x{:02x} length={}",
            frame.stream, frame.flags, frame.length
        );
    }
    println!(
        "queued={queued}: messages=2 complete_grpc_frames=true trailers=1 grpc-status=13 grpc-message=handler panicked callbacks=1 phase=Future"
    );
}

#[tokio::test]
async fn panic_after_partial_response_still_returns_internal() {
    for queued in [false, true] {
        tokio::time::timeout(WAIT, run_case(queued)).await.unwrap();
    }
}
