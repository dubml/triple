//! Connection termination, response ownership and bounded server backpressure.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
use common::{Echo, EchoService};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use triple_rpc::{
    CancellationToken, Incoming, Limits, Outgoing, RpcKind, Service, serve_connection,
};

const WAIT: Duration = Duration::from_secs(2);
async fn start<S: Service + 'static>(
    service: Arc<S>,
    timeout: Duration,
) -> (
    String,
    CancellationToken,
    JoinHandle<triple_rpc::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(io, service, Limits::default(), shutdown, timeout).await
    });
    (address, stop, server)
}
async fn raw_connect(address: &str) -> TcpStream {
    let mut io = TcpStream::connect(address).await.unwrap();
    io.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    io.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
    let mut header = [0; 9];
    io.read_exact(&mut header).await.unwrap();
    let len =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    io.read_exact(&mut vec![0; len]).await.unwrap();
    assert_eq!(header[3], 4);
    io
}
async fn joined(server: JoinHandle<triple_rpc::Result<()>>) -> triple_rpc::Result<()> {
    tokio::time::timeout(WAIT, server).await.unwrap().unwrap()
}

#[tokio::test]
async fn peer_close_without_goaway_returns_ok() {
    let (address, _, server) = start(Arc::new(EchoService), WAIT).await;
    drop(raw_connect(&address).await);
    joined(server).await.unwrap();
}
#[tokio::test]
async fn peer_goaway_then_close_returns_ok() {
    let (address, _, server) = start(Arc::new(EchoService), WAIT).await;
    let mut io = raw_connect(&address).await;
    io.write_all(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    io.shutdown().await.unwrap();
    drop(io);
    joined(server).await.unwrap();
}
#[tokio::test]
async fn peer_reset_with_open_stream_returns_ok() {
    let counts = Arc::new(Counts::default());
    let (address, _, server) = start(
        Arc::new(Flood {
            counts: counts.clone(),
            size: 0,
            block: true,
        }),
        WAIT,
    )
    .await;
    let mut io = raw_connect(&address).await;
    raw_call(&mut io).await;
    tokio::time::timeout(WAIT, counts.started.notified())
        .await
        .unwrap();
    // Abortive close while server SETTINGS ACK/response bytes remain unread.
    #[allow(deprecated)]
    io.set_linger(Some(Duration::ZERO)).unwrap();
    drop(io);
    joined(server).await.unwrap();
    assert_eq!(counts.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn local_protocol_error_still_returns_err() {
    let (address, _, server) = start(Arc::new(EchoService), WAIT).await;
    let mut io = raw_connect(&address).await;
    io.write_all(&[0, 0, 1, 0, 0, 0, 0, 0, 0, 1]).await.unwrap();
    let error = joined(server).await.unwrap_err();
    assert!(matches!(error, triple_rpc::Error::Io(_)));
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut reason = None;
    while let Some(current) = cause {
        if let Some(h2) = current.downcast_ref::<h2::Error>() {
            reason = h2.reason();
        }
        cause = current.source();
    }
    assert_eq!(reason, Some(h2::Reason::PROTOCOL_ERROR));
}

#[derive(Default)]
struct Counts {
    attempts: AtomicUsize,
    completed: AtomicUsize,
    active: AtomicUsize,
    dropped: AtomicUsize,
    started: Notify,
}
struct Guard(Arc<Counts>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Flood {
    counts: Arc<Counts>,
    size: usize,
    block: bool,
}
impl Service for Flood {
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
        request.message::<Echo>().await?;
        self.counts.started.notify_one();
        if self.block {
            std::future::pending::<()>().await;
        }
        let message = Echo {
            text: "x".repeat(self.size),
        };
        loop {
            self.counts.attempts.fetch_add(1, Ordering::SeqCst);
            response.send(&message).await?;
            self.counts.completed.fetch_add(1, Ordering::SeqCst);
        }
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

#[tokio::test]
async fn response_backpressure_reaches_handler() {
    for size in [8 * 1024, 32 * 1024] {
        let counts = Arc::new(Counts::default());
        let (address, stop, server) = start(
            Arc::new(Flood {
                counts: counts.clone(),
                size,
                block: false,
            }),
            Duration::from_millis(50),
        )
        .await;
        let mut client = triple_rpc::Client::connect(&address, Limits::default())
            .await
            .unwrap();
        let triple_rpc::Call {
            mut requests,
            response,
        } = client
            .call_stream(
                "/test.Flow/Flood",
                RpcKind::ServerStreaming,
                &triple_rpc::Metadata::new(),
            )
            .await
            .unwrap();
        requests
            .send(&Echo {
                text: String::new(),
            })
            .await
            .unwrap();
        requests.close().unwrap();
        let mut incoming = response.receive().await.unwrap();
        counts.started.notified().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let frame_len = triple_codec::encode_frame(
            &triple_rpc::encode_message(
                &Echo {
                    text: "x".repeat(size),
                },
                Limits::default().max_message_size,
            )
            .unwrap(),
            Limits::default().max_message_size,
        )
        .unwrap()
        .len();
        let completed = counts.completed.load(Ordering::SeqCst);
        let attempts = counts.attempts.load(Ordering::SeqCst);
        let unsent = (completed * frame_len).saturating_sub(65_535);
        println!(
            "size={size} frame_len={frame_len} attempts={attempts} completed={completed} unsent={unsent} bound={}",
            3 * frame_len
        );
        assert!(unsent <= 3 * frame_len);
        assert!(attempts > completed);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(counts.completed.load(Ordering::SeqCst), completed);
        assert_eq!(counts.attempts.load(Ordering::SeqCst), attempts);
        assert_eq!(counts.active.load(Ordering::SeqCst), 1);
        println!("after 2s: attempts={attempts} completed={completed} unsent={unsent}");
        tokio::time::timeout(WAIT, async {
            while counts.completed.load(Ordering::SeqCst) <= completed {
                assert!(incoming.message::<Echo>().await.unwrap().is_some());
            }
        })
        .await
        .unwrap();
        drop(incoming);
        stop.cancel();
        joined(server).await.unwrap();
        drop(client);
    }
}

#[tokio::test]
async fn serve_connection_returns_ok_when_peer_drops_with_unread_server_data() {
    let counts = Arc::new(Counts::default());
    let (address, _, server) = start(
        Arc::new(Flood {
            counts: counts.clone(),
            size: 32 * 1024,
            block: false,
        }),
        WAIT,
    )
    .await;
    let mut io = raw_connect(&address).await;
    raw_call(&mut io).await;
    tokio::time::timeout(WAIT, counts.started.notified())
        .await
        .unwrap();
    wait_for(|| counts.completed.load(Ordering::SeqCst) >= 1).await;
    drop(io);
    joined(server).await.unwrap();
    assert_eq!(counts.active.load(Ordering::SeqCst), 0);
}

async fn raw_call(io: &mut TcpStream) {
    // Literal non-indexed pseudo/protocol fields; never start a client reader.
    let mut block = vec![0x83, 0x86, 0x04, 16];
    block.extend_from_slice(b"/test.Flow/Flood");
    block.extend_from_slice(&[0, 12]);
    block.extend_from_slice(b"content-type");
    block.push(16);
    block.extend_from_slice(b"application/grpc");
    block.extend_from_slice(&[0, 2, b't', b'e', 8]);
    block.extend_from_slice(b"trailers");
    let mut header = vec![0, 0, u8::try_from(block.len()).unwrap(), 1, 4, 0, 0, 0, 1];
    header.extend(block);
    io.write_all(&header).await.unwrap();
    io.write_all(&[0, 0, 5, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0])
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_timeout_joins_hyper_stream_tasks() {
    let counts = Arc::new(Counts::default());
    let (address, stop, server) = start(
        Arc::new(Flood {
            counts: counts.clone(),
            size: 0,
            block: true,
        }),
        Duration::from_millis(30),
    )
    .await;
    let mut client = triple_rpc::Client::connect(&address, Limits::default())
        .await
        .unwrap();
    let mut calls = Vec::new();
    for _ in 0..3 {
        let triple_rpc::Call {
            mut requests,
            response,
        } = client
            .call_stream(
                "/test.Flow/Block",
                RpcKind::ServerStreaming,
                &triple_rpc::Metadata::new(),
            )
            .await
            .unwrap();
        requests
            .send(&Echo {
                text: String::new(),
            })
            .await
            .unwrap();
        requests.close().unwrap();
        calls.push(response.receive().await.unwrap());
        counts.started.notified().await;
    }
    assert_eq!(counts.active.load(Ordering::SeqCst), 3);
    stop.cancel();
    joined(server).await.unwrap();
    assert_eq!(counts.active.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn responses_have_no_date_header() {
    let (address, stop, server) = start(Arc::new(EchoService), WAIT).await;
    let mut client = triple_transport::Client::connect(&address, Limits::default())
        .await
        .unwrap();
    client.ready().await.unwrap();
    let request = triple_rpc::prepare_request(
        &format!("http://{address}").parse().unwrap(),
        "/test.Echo/Unary",
        &triple_rpc::Metadata::new(),
        None,
        Limits::default(),
    )
    .unwrap();
    let (response, stream) = client.send_request(request, false).unwrap();
    let mut outgoing = Outgoing::new(
        stream,
        Limits::default(),
        true,
        triple_rpc::SendRole::Request,
    );
    outgoing
        .send(&Echo {
            text: "date".into(),
        })
        .await
        .unwrap();
    outgoing.close().unwrap();
    let response = response.await.unwrap();
    assert!(!response.headers().contains_key("date"));
    assert_eq!(response.headers()["content-type"], "application/grpc+proto");
    drop(response);
    stop.cancel();
    joined(server).await.unwrap();
}

#[tokio::test]
async fn handler_future_is_dropped_when_response_body_is_dropped() {
    let counts = Arc::new(Counts::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = TaskTracker::new();
    let tasks = tracker.clone();
    let (sent, mut received) = tokio::sync::mpsc::channel(1);
    let service = Arc::new(Flood {
        counts: counts.clone(),
        size: 0,
        block: true,
    });
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let connection =
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(
                    hyper_util::rt::TokioIo::new(io),
                    hyper::service::service_fn(move |request| {
                        let (tasks, service, sent) = (tasks.clone(), service.clone(), sent.clone());
                        async move {
                            let response =
                                triple_rpc::handle(request, service, Limits::default(), tasks)
                                    .await
                                    .unwrap();
                            sent.send(response.into_body()).await.unwrap();
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(
                                http_body_util::Full::new(bytes::Bytes::new()),
                            ))
                        }
                    }),
                );
        tokio::pin!(connection);
        tokio::select! { result = &mut connection => result.unwrap(), _ = shutdown.cancelled() => {} }
    });
    let mut client = triple_rpc::Client::connect(&address.to_string(), Limits::default())
        .await
        .unwrap();
    let triple_rpc::Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Flow/Block",
            RpcKind::ServerStreaming,
            &triple_rpc::Metadata::new(),
        )
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: String::new(),
        })
        .await
        .unwrap();
    requests.close().unwrap();
    let body = received.recv().await.unwrap();
    counts.started.notified().await;
    drop(body);
    wait_for(|| counts.dropped.load(Ordering::SeqCst) == 1).await;
    tracker.close();
    tokio::time::timeout(WAIT, tracker.wait()).await.unwrap();
    stop.cancel();
    server.await.unwrap();
    drop(response);
}
