//! Cancellation distinguishes incomplete prefaces from established HTTP/2.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rpc::{CancellationToken, Incoming, Limits, Outgoing, RpcKind, Service};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::Notify,
    task::JoinHandle,
};

const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const WAIT: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Counts {
    reads: AtomicUsize,
    writes: AtomicUsize,
    bytes: AtomicUsize,
    changed: Notify,
    calls: AtomicUsize,
    started: Notify,
    dropped: AtomicUsize,
}
struct ObservedIo(DuplexStream, Arc<Counts>);
impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.1.reads.fetch_add(1, SeqCst);
        let filled = buf.filled().len();
        let result = Pin::new(&mut self.0).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.1.bytes.fetch_add(buf.filled().len() - filled, SeqCst);
            self.1.changed.notify_one();
        }
        result
    }
}
impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.1.writes.fetch_add(1, SeqCst);
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
struct Guard(Arc<Counts>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, SeqCst);
    }
}
struct Held(Arc<Counts>);
impl Service for Held {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::Bidirectional)
    }
    async fn call(&self, _: &str, _: &mut Incoming, _: &mut Outgoing) -> rpc::Result<()> {
        self.0.calls.fetch_add(1, SeqCst);
        let _guard = Guard(self.0.clone());
        self.0.started.notify_one();
        std::future::pending().await
    }
}
fn start(
    io: DuplexStream,
    counts: Arc<Counts>,
    stop: CancellationToken,
    budget: Duration,
) -> JoinHandle<rpc::Result<()>> {
    tokio::spawn(rpc::serve_connection(
        ObservedIo(io, counts.clone()),
        Arc::new(Held(counts)),
        Limits::default(),
        stop,
        budget,
    ))
}
async fn joined(server: JoinHandle<rpc::Result<()>>) -> rpc::Result<()> {
    tokio::time::timeout(WAIT, server).await.unwrap().unwrap()
}
async fn read_bytes(counts: &Counts, expected: usize) {
    tokio::time::timeout(WAIT, async {
        while counts.bytes.load(SeqCst) < expected {
            counts.changed.notified().await;
        }
    })
    .await
    .unwrap();
}
async fn frame(peer: &mut DuplexStream) -> (u8, u8, Vec<u8>) {
    let mut header = [0; 9];
    peer.read_exact(&mut header).await.unwrap();
    let len =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let mut data = vec![0; len];
    peer.read_exact(&mut data).await.unwrap();
    (header[3], header[4], data)
}

#[tokio::test]
async fn pre_cancelled_handshake_performs_no_io() {
    let (io, _peer) = tokio::io::duplex(64);
    let counts = Arc::new(Counts::default());
    let stop = CancellationToken::new();
    stop.cancel();
    let started = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(1),
        start(io, counts.clone(), stop, Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    println!(
        "pre-cancelled: elapsed_ms={:.3} read_polls={} write_polls={}",
        started.elapsed().as_secs_f64() * 1000.0,
        counts.reads.load(SeqCst),
        counts.writes.load(SeqCst)
    );
    assert_eq!(counts.reads.load(SeqCst), 0);
    assert_eq!(counts.writes.load(SeqCst), 0);
    assert_eq!(counts.calls.load(SeqCst), 0);
}

#[tokio::test]
async fn partial_preface_cancellation_does_not_drain() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let counts = Arc::new(Counts::default());
    let stop = CancellationToken::new();
    let server = start(io, counts.clone(), stop.clone(), Duration::from_secs(1));
    peer.write_all(&PREFACE[..12]).await.unwrap();
    read_bytes(&counts, 12).await;
    let started = Instant::now();
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(500), joined(server))
        .await
        .unwrap()
        .unwrap();
    println!(
        "partial 12 bytes: elapsed_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    assert_eq!(counts.bytes.load(SeqCst), 12);
    assert_eq!(counts.calls.load(SeqCst), 0);
}

#[tokio::test]
async fn complete_preface_without_streams_preserves_drain_budget() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let counts = Arc::new(Counts::default());
    let stop = CancellationToken::new();
    let budget = Duration::from_millis(200);
    let server = start(io, counts.clone(), stop.clone(), budget);
    // Force 24 separate reads to verify matching across poll_read boundaries.
    for (index, byte) in PREFACE.iter().enumerate() {
        peer.write_all(std::slice::from_ref(byte)).await.unwrap();
        read_bytes(&counts, index + 1).await;
    }
    peer.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
    peer.write_all(&[0, 0, 8, 6, 0, 0, 0, 0, 0]).await.unwrap();
    peer.write_all(b"verified").await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            let (kind, flags, data) = frame(&mut peer).await;
            if kind == 6 && flags == 1 && data == b"verified" {
                break;
            }
        }
    })
    .await
    .unwrap();
    // A PING ACK proves the handshake finished without accepting a stream.
    // Stop reading/acknowledging so graceful shutdown must use its budget.
    let started = Instant::now();
    stop.cancel();
    joined(server).await.unwrap();
    println!(
        "complete preface, no streams: elapsed_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    assert!(started.elapsed() >= budget);
    assert_eq!(counts.calls.load(SeqCst), 0);
}

#[tokio::test]
async fn invalid_preface_preserves_hyper_protocol_error() {
    let (io, mut peer) = tokio::io::duplex(1024);
    let counts = Arc::new(Counts::default());
    let server = start(
        io,
        counts.clone(),
        CancellationToken::new(),
        Duration::from_secs(1),
    );
    peer.write_all(b"GET * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let error = joined(server).await.unwrap_err();
    println!("invalid preface: result={error:?}");
    assert!(matches!(error, rpc::Error::Io(_)));
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut reason = None;
    while let Some(current) = cause {
        if let Some(h2) = current.downcast_ref::<h2::Error>() {
            reason = h2.reason();
        }
        cause = current.source();
    }
    assert_eq!(reason, Some(h2::Reason::PROTOCOL_ERROR));
    assert_eq!(counts.calls.load(SeqCst), 0);
}

#[tokio::test]
async fn established_call_preserves_drain_budget_and_joins_handler() {
    let (io, peer) = tokio::io::duplex(4096);
    let counts = Arc::new(Counts::default());
    let stop = CancellationToken::new();
    let budget = Duration::from_millis(200);
    let server = start(io, counts.clone(), stop.clone(), budget);
    let mut client = rpc::Client::from_io(peer, "http://localhost", Limits::default())
        .await
        .unwrap();
    let call = client
        .call_stream("/test/Held", RpcKind::Bidirectional, &rpc::Metadata::new())
        .await
        .unwrap();
    let mut incoming = call.response.receive().await.unwrap();
    tokio::time::timeout(WAIT, counts.started.notified())
        .await
        .unwrap();
    let started = Instant::now();
    stop.cancel();
    joined(server).await.unwrap();
    println!(
        "active call: elapsed_ms={:.3} handlers_dropped={}",
        started.elapsed().as_secs_f64() * 1000.0,
        counts.dropped.load(SeqCst)
    );
    assert!(started.elapsed() >= budget);
    assert_eq!(counts.calls.load(SeqCst), 1);
    assert_eq!(counts.dropped.load(SeqCst), 1);
    #[derive(prost::Message)]
    struct Empty {}
    assert!(
        tokio::time::timeout(WAIT, incoming.message::<Empty>())
            .await
            .unwrap()
            .is_err()
    );
}
