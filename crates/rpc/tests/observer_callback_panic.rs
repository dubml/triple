//! Characterize an observer which panics while reporting an already panicking handler.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use prost::Message;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::task::TaskTracker;
use triple_rpc::{
    CancellationToken, HandlerPanicPhase, Incoming, Limits, Metadata, Outgoing, RpcKind, Service,
};

const WAIT: Duration = Duration::from_secs(2);
const SHUTDOWN: Duration = Duration::from_millis(40);

#[derive(Clone, PartialEq, Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Default)]
struct Work {
    callbacks: AtomicUsize,
    held: AtomicUsize,
    dropped: AtomicUsize,
    started: Notify,
}

struct Held<'a>(&'a Work);
impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, SeqCst);
        self.0.dropped.fetch_add(1, SeqCst);
    }
}

impl Service for Work {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::Bidirectional)
    }
    fn on_handler_panic(&self, _: &str, _: HandlerPanicPhase) {
        self.callbacks.fetch_add(1, SeqCst);
        panic!("observer callback panic");
    }
    fn call(
        &self,
        path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> impl Future<Output = triple_rpc::Result<()>> + Send {
        if path.ends_with("Construction") {
            panic!("handler construction panic");
        }
        async move {
            if path.ends_with("Future") {
                panic!("handler future panic");
            }
            if path.ends_with("Held") {
                self.held.fetch_add(1, SeqCst);
                let _guard = Held(self);
                self.started.notify_one();
                std::future::pending().await
            } else {
                let message = request.message::<Echo>().await?.unwrap();
                response.send(&message).await
            }
        }
    }
}

// Inspection counterpart of hyper_server::serve_tracked_connection's builder,
// handle and stop.cancel/close/wait sequence. These runs cancel only after a
// completed handshake; the preface wrapper is irrelevant to this observation.
// The other run uses the public serve_connection, whose tracker is private.
async fn observed_connection(
    io: TcpStream,
    work: Arc<Work>,
    shutdown: CancellationToken,
    tracker: TaskTracker,
) -> triple_rpc::Result<()> {
    let stop = CancellationToken::new();
    let mut builder = hyper::server::conn::http2::Builder::new(triple_rpc::StreamExecutor::new(
        tracker.clone(),
        stop.clone(),
    ));
    let limits = Limits::default();
    let s = triple_rpc::http2_settings(limits);
    builder
        .initial_stream_window_size(s.initial_stream_window_size)
        .initial_connection_window_size(s.initial_connection_window_size)
        .max_frame_size(s.max_frame_size)
        .max_concurrent_streams(s.max_concurrent_streams)
        .max_header_list_size(u32::try_from(s.max_header_list_size).unwrap())
        .max_send_buf_size(s.max_send_buf_size)
        .header_table_size(s.header_table_size)
        .max_pending_accept_reset_streams(s.max_pending_accept_reset_streams)
        .max_local_error_reset_streams(s.max_local_error_reset_streams)
        .keep_alive_interval(s.keep_alive_interval)
        .keep_alive_timeout(s.keep_alive_timeout)
        .adaptive_window(false)
        .auto_date_header(false);
    let calls = tracker.clone();
    let connection = builder.serve_connection(
        hyper_util::rt::TokioIo::new(io),
        hyper::service::service_fn(move |request| {
            triple_rpc::handle(request, work.clone(), limits, calls.clone())
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

#[tokio::test]
async fn observer_callback_panic_behavior_is_characterized() {
    for observed in [false, true] {
        for phase in ["Construction", "Future"] {
            tokio::time::timeout(WAIT, async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let work = Arc::new(Work::default());
                let shutdown = CancellationToken::new();
                let tracker = TaskTracker::new();
                let (service, stop, tasks) = (work.clone(), shutdown.clone(), tracker.clone());
                let server = tokio::spawn(async move {
                    let (io, _) = listener.accept().await.unwrap();
                    if observed {
                        observed_connection(io, service, stop, tasks).await
                    } else {
                        triple_rpc::serve_connection(io, service, Limits::default(), stop, SHUTDOWN).await
                    }
                });
                let mut client = triple_rpc::Client::connect(&address, Limits::default()).await.unwrap();
                let triple_rpc::Call { requests: held_requests, response: held_response } = client
                    .call_stream("/test.Observer/Held", RpcKind::Bidirectional, &Metadata::new())
                    .await.unwrap();
                work.started.notified().await;
                let held_incoming = held_response.receive().await.unwrap();
                let triple_rpc::Call { mut requests, response } = client
                    .call_stream(&format!("/test.Observer/{phase}"), RpcKind::Bidirectional, &Metadata::new())
                    .await.unwrap();
                requests.close().unwrap();
                let result = async {
                    let mut incoming = response.receive().await?;
                    incoming.message::<Echo>().await
                }.await;
                assert!(matches!(result, Err(triple_rpc::Error::Transport(triple_transport::Error::Transport(ref error)))
                    if error.reason() == Some(h2::Reason::CANCEL)), "{result:?}");
                assert_eq!(work.callbacks.load(SeqCst), 1);
                assert_eq!(work.held.load(SeqCst), 1);
                assert_eq!(work.dropped.load(SeqCst), 0);
                assert!(!server.is_finished());
                let echo: Echo = client.unary("/test.Observer/Echo", &Echo { text: "alive".into() }, &Metadata::new())
                    .await.unwrap();
                assert_eq!(echo.text, "alive");
                println!("observer_panic observed={observed} phase={phase}: client={result:?}, sibling_active=1 sibling_dropped=0 next_call=alive serve_pending=true callbacks=1");
                shutdown.cancel();
                let result = server.await.unwrap();
                assert!(result.is_ok(), "{result:?}");
                assert_eq!(work.held.load(SeqCst), 0);
                assert_eq!(work.dropped.load(SeqCst), 1);
                if observed {
                    assert!(tracker.is_closed());
                    assert!(tracker.is_empty());
                }
                println!("observer_panic final observed={observed} phase={phase}: serve={result:?}, sibling_joined=true tracker_empty={:?}", observed.then(|| tracker.is_empty()));
                drop((held_requests, held_incoming, client));
            }).await.unwrap();
        }
    }
}
