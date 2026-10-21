//! Characterizes server cleanup and the panic-contract difference from transport.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    task::{Context, Poll},
    time::Duration,
};

use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use triple_rpc::{CancellationToken, Incoming, Limits, Metadata, Outgoing, RpcKind, Service};

const WAIT: Duration = Duration::from_secs(2);
const SHUTDOWN: Duration = Duration::from_millis(40);

#[derive(Clone, PartialEq, Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Default)]
struct Counts {
    active: AtomicUsize,
    dropped: AtomicUsize,
    constructed: AtomicUsize,
    polled: AtomicUsize,
    unwound: AtomicUsize,
}

struct Guard(Arc<Counts>);
impl Guard {
    fn new(counts: &Arc<Counts>) -> Self {
        counts.active.fetch_add(1, SeqCst);
        Self(counts.clone())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, SeqCst);
        self.0.dropped.fetch_add(1, SeqCst);
        if std::thread::panicking() {
            self.0.unwound.fetch_add(1, SeqCst);
        }
    }
}

#[derive(Default)]
struct Work {
    held: Arc<Counts>,
    panic: Arc<Counts>,
    calls: AtomicUsize,
}
impl Service for Work {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::Bidirectional)
    }
    // Deliberately not `async fn`: FactoryPanic unwinds while constructing the
    // handler future; FuturePanic unwinds only when that future is polled.
    fn call(
        &self,
        path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> impl Future<Output = triple_rpc::Result<()>> + Send {
        self.calls.fetch_add(1, SeqCst);
        if path.ends_with("FactoryPanic") {
            let _guard = Guard::new(&self.panic);
            self.panic.constructed.fetch_add(1, SeqCst);
            panic!("intentional handler construction panic");
        }
        async move {
            if path.ends_with("FuturePanic") {
                let _guard = Guard::new(&self.panic);
                self.panic.constructed.fetch_add(1, SeqCst);
                self.panic.polled.fetch_add(1, SeqCst);
                panic!("intentional handler future panic");
            }
            let _guard = Guard::new(&self.held);
            let message = request
                .message::<Echo>()
                .await?
                .ok_or(triple_rpc::Error::Cardinality("request"))?;
            if path.ends_with("Held") {
                std::future::pending().await
            } else {
                response.send(&message).await
            }
        }
    }
}

#[derive(Default)]
struct IoCounts {
    reads: AtomicUsize,
    writes: AtomicUsize,
    dropped: AtomicUsize,
}
struct ObservedIo<T>(T, Arc<IoCounts>);
impl<T> Drop for ObservedIo<T> {
    fn drop(&mut self) {
        self.1.dropped.fetch_add(1, SeqCst);
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for ObservedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.1.reads.fetch_add(1, SeqCst);
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for ObservedIo<T> {
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

#[derive(Clone, Default)]
struct Inspection {
    tracker: TaskTracker,
    streams: Arc<Counts>,
}
#[derive(Clone)]
struct CountingExecutor {
    inner: triple_rpc::StreamExecutor,
    streams: Arc<Counts>,
}
impl<F> hyper::rt::Executor<F> for CountingExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        self.streams.constructed.fetch_add(1, SeqCst);
        let guard = Guard::new(&self.streams);
        self.inner.execute(async move {
            let _guard = guard;
            future.await
        });
    }
}

// Inspection-only counterpart of hyper_server.rs:258-322: same settings,
// StreamExecutor, handle, graceful timeout, stop.cancel/close/wait sequence.
// The public serve_connection owns a private tracker; this additional run makes
// its task counts visible without adding cfg(test) state to production structs.
// Only normal graceful termination is compared here: connection_result's peer
// I/O normalization is intentionally left to the real public-entry tests.
async fn observed_connection<T>(
    io: T,
    service: Arc<Work>,
    limits: Limits,
    shutdown: CancellationToken,
    inspection: Inspection,
) -> triple_rpc::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let tracker = inspection.tracker;
    let stop = CancellationToken::new();
    let executor = CountingExecutor {
        inner: triple_rpc::StreamExecutor::new(tracker.clone(), stop.clone()),
        streams: inspection.streams,
    };
    let mut builder = hyper::server::conn::http2::Builder::new(executor);
    let s = triple_rpc::http2_settings(limits);
    let header_limit = u32::try_from(s.max_header_list_size)
        .map_err(|_| triple_transport::Error::InvalidConfig("header limit"))?;
    builder
        .max_header_list_size(header_limit)
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
    // Mirror the public entry's pre-cancelled case without polling hyper. The
    // partial/complete-preface cases exercise the real entry in handshake tests.
    if shutdown.is_cancelled() {
        stop.cancel();
        tracker.close();
        tracker.wait().await;
        return Ok(());
    }
    let calls = tracker.clone();
    let connection = builder.serve_connection(
        hyper_util::rt::TokioIo::new(io),
        hyper::service::service_fn(move |request| {
            triple_rpc::handle(request, service.clone(), limits, calls.clone())
        }),
    );
    tokio::pin!(connection);
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            match tokio::time::timeout(SHUTDOWN, &mut connection).await {
                Ok(result) => result,
                Err(_) => Ok(()),
            }
        },
        result = &mut connection => result,
    };
    stop.cancel();
    tracker.close();
    tracker.wait().await;
    result.map_err(|error| triple_rpc::Error::Io(std::io::Error::other(error)))
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

async fn start(
    observed: bool,
) -> (
    String,
    CancellationToken,
    Arc<Work>,
    Inspection,
    JoinHandle<triple_rpc::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let service = Arc::new(Work::default());
    let inspection = Inspection::default();
    let (shutdown, work, tasks) = (stop.clone(), service.clone(), inspection.clone());
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        if observed {
            observed_connection(io, work, Limits::default(), shutdown, tasks).await
        } else {
            triple_rpc::serve_connection(io, work, Limits::default(), shutdown, SHUTDOWN).await
        }
    });
    (address, stop, service, inspection, server)
}

async fn held(client: &mut triple_rpc::Client, work: &Work) -> (Outgoing, Incoming) {
    let triple_rpc::Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Contract/Held",
            RpcKind::Bidirectional,
            &Metadata::new(),
        )
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "held".into(),
        })
        .await
        .unwrap();
    let incoming = response.receive().await.unwrap();
    wait_for(|| work.held.active.load(SeqCst) == 1).await;
    (requests, incoming)
}

#[tokio::test]
async fn connection_loss_joins_handlers_and_fails_both_halves() {
    let (address, _, work, _, server) = start(false).await;
    let mut client = triple_rpc::Client::connect(&address, Limits::default())
        .await
        .unwrap();
    let (mut outgoing, mut incoming) = held(&mut client, &work).await;
    drop(client);
    let result = tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(work.held.active.load(SeqCst), 0);
    assert_eq!(work.held.dropped.load(SeqCst), 1);
    let read = tokio::time::timeout(WAIT, incoming.message::<Echo>())
        .await
        .unwrap();
    let write = tokio::time::timeout(
        WAIT,
        outgoing.send(&Echo {
            text: "late".into(),
        }),
    )
    .await
    .unwrap();
    println!(
        "G1 public: server={result:?}, handlers_active=0 dropped=1 read={read:?} write={write:?}"
    );
    assert!(read.is_err());
    assert!(write.is_err());
}

#[tokio::test]
async fn handler_panics_report_failure_and_join_siblings() {
    // Old transport returns HandlerTask and terminates sibling handlers. RPC
    // isolates the failed call, sends INTERNAL, and invokes on_handler_panic;
    // callback path/phase/count are covered by handler_panic.rs. Keep the legacy name.
    for factory in [true, false] {
        for observed in [false, true] {
            let (address, stop, work, inspection, mut server) = start(observed).await;
            let tracker = &inspection.tracker;
            let mut client = triple_rpc::Client::connect(&address, Limits::default())
                .await
                .unwrap();
            let (_held_send, _held_receive) = held(&mut client, &work).await;
            if observed {
                wait_for(|| tracker.len() == 2).await;
            }
            let path = if factory {
                "/test.Contract/FactoryPanic"
            } else {
                "/test.Contract/FuturePanic"
            };
            let triple_rpc::Call {
                mut requests,
                response,
            } = client
                .call_stream(path, RpcKind::Bidirectional, &Metadata::new())
                .await
                .unwrap();
            requests.close().unwrap();
            let failed = tokio::time::timeout(WAIT, async {
                let mut incoming = response.receive().await?;
                incoming.message::<Echo>().await
            })
            .await
            .unwrap();
            wait_for(|| work.panic.unwound.load(SeqCst) == 1).await;
            assert!(
                matches!(
                    failed,
                    Err(triple_rpc::Error::Rpc(ref status))
                        if status.code() == triple_rpc::Code::Internal
                            && status.message() == "handler panicked"
                ),
                "{failed:?}"
            );
            assert_eq!(work.panic.constructed.load(SeqCst), 1);
            assert_eq!(work.panic.polled.load(SeqCst), usize::from(!factory));
            // Give a fatal connection result a bounded opportunity to surface.
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut server)
                    .await
                    .is_err()
            );
            assert_eq!(work.held.active.load(SeqCst), 1);
            assert_eq!(work.held.dropped.load(SeqCst), 0);
            let echo: Echo = client
                .unary(
                    "/test.Contract/Echo",
                    &Echo {
                        text: "alive".into(),
                    },
                    &Metadata::new(),
                )
                .await
                .unwrap();
            assert_eq!(echo.text, "alive");
            if observed {
                wait_for(|| tracker.len() == 2 && inspection.streams.active.load(SeqCst) == 1)
                    .await;
                assert_eq!(inspection.streams.constructed.load(SeqCst), 3);
                assert_eq!(inspection.streams.unwound.load(SeqCst), 0);
            }
            println!(
                "G2 factory={factory} observed_tracker={observed}: unwound=1 panic_rpc={failed:?}, server_pending=true sibling_active=1 sibling_dropped=0 next_call=alive tracker_len={:?}; panic isolated with INTERNAL and callback",
                observed.then(|| tracker.len())
            );
            stop.cancel();
            let result = tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(work.held.active.load(SeqCst), 0);
            // The echo probe also used a normal handler guard.
            assert_eq!(work.held.dropped.load(SeqCst), 2);
            if observed {
                assert!(tracker.is_closed());
                assert!(tracker.is_empty());
                assert_eq!(inspection.streams.active.load(SeqCst), 0);
            }
            println!(
                "G2 final factory={factory} observed_tracker={observed}: server={result:?}, sibling_joined_after_shutdown=true tracker_empty={:?}",
                observed.then(|| tracker.is_empty())
            );
        }
    }
}

#[tokio::test]
async fn pre_cancelled_plain_handshake_starts_no_tasks() {
    // Preserve the eight successful-cancellation/drop trials on the current
    // server. The old implementation's observations are archived in 4b9d7f4.
    // 预取消握手不再进行 I/O 轮询，因为包装器外层在 hyper 之前观察到取消。
    for trial in 0..8 {
        let (io, _peer) = tokio::io::duplex(64);
        let trial_io = Arc::new(IoCounts::default());
        let stop = CancellationToken::new();
        stop.cancel();
        let result = tokio::time::timeout(
            WAIT,
            triple_rpc::serve_connection(
                ObservedIo(io, trial_io.clone()),
                Arc::new(Work::default()),
                Limits::default(),
                stop,
                SHUTDOWN,
            ),
        )
        .await
        .unwrap();
        println!(
            "G3 rpc trial={trial}: result={result:?} read_polls={} write_polls={} io_dropped={}",
            trial_io.reads.load(SeqCst),
            trial_io.writes.load(SeqCst),
            trial_io.dropped.load(SeqCst)
        );
        assert!(result.is_ok());
        assert_eq!(trial_io.dropped.load(SeqCst), 1);
    }
    for observed in [false, true] {
        let (io, _peer) = tokio::io::duplex(64);
        let counts = Arc::new(IoCounts::default());
        let io = ObservedIo(io, counts.clone());
        let work = Arc::new(Work::default());
        let inspection = Inspection::default();
        let stop = CancellationToken::new();
        stop.cancel();
        let result = tokio::time::timeout(WAIT, async {
            if observed {
                observed_connection(
                    io,
                    work.clone(),
                    Limits::default(),
                    stop,
                    inspection.clone(),
                )
                .await
            } else {
                triple_rpc::serve_connection(io, work.clone(), Limits::default(), stop, SHUTDOWN)
                    .await
            }
        })
        .await
        .unwrap();
        println!(
            "G3 observed_tracker={observed}: result={result:?} read_polls={} write_polls={} io_dropped={} handler_calls={} tracker_len={:?} streams_created={:?}",
            counts.reads.load(SeqCst),
            counts.writes.load(SeqCst),
            counts.dropped.load(SeqCst),
            work.calls.load(SeqCst),
            observed.then(|| inspection.tracker.len()),
            observed.then(|| inspection.streams.constructed.load(SeqCst))
        );
        assert!(result.is_ok());
        assert_eq!(counts.reads.load(SeqCst), 0);
        assert_eq!(counts.writes.load(SeqCst), 0);
        assert_eq!(work.calls.load(SeqCst), 0);
        assert_eq!(counts.dropped.load(SeqCst), 1);
        if observed {
            assert!(inspection.tracker.is_closed());
            assert!(inspection.tracker.is_empty());
            assert_eq!(inspection.streams.constructed.load(SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn invalid_header_limit_starts_no_connection_tasks() {
    for observed in [false, true] {
        let (io, _peer) = tokio::io::duplex(64);
        let counts = Arc::new(IoCounts::default());
        let io = ObservedIo(io, counts.clone());
        let work = Arc::new(Work::default());
        let inspection = Inspection::default();
        let limits = Limits {
            max_header_size: usize::MAX,
            ..Limits::default()
        };
        let result = if observed {
            observed_connection(
                io,
                work.clone(),
                limits,
                CancellationToken::new(),
                inspection.clone(),
            )
            .await
        } else {
            triple_rpc::serve_connection(
                io,
                work.clone(),
                limits,
                CancellationToken::new(),
                SHUTDOWN,
            )
            .await
        };
        println!(
            "G4 observed_tracker={observed}: result={result:?} read_polls={} write_polls={} io_dropped={} handler_calls={} tracker_len={:?} streams_created={:?}",
            counts.reads.load(SeqCst),
            counts.writes.load(SeqCst),
            counts.dropped.load(SeqCst),
            work.calls.load(SeqCst),
            observed.then(|| inspection.tracker.len()),
            observed.then(|| inspection.streams.constructed.load(SeqCst))
        );
        assert!(matches!(
            result,
            Err(triple_rpc::Error::Transport(
                triple_transport::Error::InvalidConfig("header limit")
            ))
        ));
        assert_eq!(counts.reads.load(SeqCst), 0);
        assert_eq!(counts.writes.load(SeqCst), 0);
        assert_eq!(counts.dropped.load(SeqCst), 1);
        assert_eq!(work.calls.load(SeqCst), 0);
        if observed {
            assert!(inspection.tracker.is_empty());
            assert_eq!(inspection.streams.constructed.load(SeqCst), 0);
        }
    }
}
