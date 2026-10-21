//! Handler panics remain local to one RPC and are observable without their payload.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use prost::Message;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use triple_rpc::{
    CallOptions, CancellationToken, Client, Code, Deadline, HandlerPanicPhase, Incoming, Limits,
    Metadata, Outgoing, RpcKind, Service, Status,
};

const WAIT: Duration = Duration::from_secs(2);
const SECRET: &str = "private handler panic payload";
const CASES: [(&str, HandlerPanicPhase); 2] = [
    ("/test.Panic/Construction", HandlerPanicPhase::Construction),
    ("/test.Panic/Future", HandlerPanicPhase::Future),
];

#[derive(Clone, PartialEq, Message)]
struct Echo {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Default)]
struct Events {
    panics: Mutex<Vec<(String, HandlerPanicPhase)>>,
    active: AtomicUsize,
    dropped: AtomicUsize,
    started: Notify,
}

struct Held(Arc<Events>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, SeqCst);
        self.0.dropped.fetch_add(1, SeqCst);
    }
}

struct Work(Arc<Events>);
impl Service for Work {
    fn contains(&self, _: &str) -> bool {
        true
    }

    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::Bidirectional)
    }

    fn on_handler_panic(&self, path: &str, phase: HandlerPanicPhase) {
        self.0.panics.lock().unwrap().push((path.to_owned(), phase));
    }

    fn call(
        &self,
        path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> impl Future<Output = triple_rpc::Result<()>> + Send {
        if path.ends_with("Construction") {
            panic!("{SECRET}: construction");
        }
        async move {
            if path.ends_with("Future") {
                panic!("{SECRET}: future");
            }
            let message = request
                .message::<Echo>()
                .await?
                .ok_or(triple_rpc::Error::Cardinality("request"))?;
            if path.ends_with("Held") {
                self.0.active.fetch_add(1, SeqCst);
                let _held = Held(self.0.clone());
                self.0.started.notify_one();
                std::future::pending().await
            } else {
                response.send(&message).await
            }
        }
    }
}

struct Server {
    address: String,
    stop: CancellationToken,
    events: Arc<Events>,
    task: JoinHandle<triple_rpc::Result<()>>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = CancellationToken::new();
        let events = Arc::new(Events::default());
        let service = Arc::new(Work(events.clone()));
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            let (io, _) = listener.accept().await?;
            triple_rpc::serve_connection(
                io,
                service,
                Limits::default(),
                shutdown,
                Duration::from_millis(40),
            )
            .await
        });
        Self {
            address,
            stop,
            events,
            task,
        }
    }

    async fn client(&self) -> Client {
        Client::connect(&self.address, Limits::default())
            .await
            .unwrap()
    }

    async fn finish(self) {
        self.stop.cancel();
        let result = tokio::time::timeout(WAIT, self.task)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(self.events.active.load(SeqCst), 0);
    }

    async fn held(&self, client: &mut Client, options: &CallOptions) -> (Outgoing, Incoming) {
        let triple_rpc::Call {
            mut requests,
            response,
        } = client
            .call_with_options("/test.Panic/Held", RpcKind::Bidirectional, options)
            .await
            .unwrap();
        requests
            .send(&Echo {
                text: "held".into(),
            })
            .await
            .unwrap();
        let incoming = response.receive().await.unwrap();
        tokio::time::timeout(WAIT, self.events.started.notified())
            .await
            .unwrap();
        (requests, incoming)
    }
}

async fn panic_status(client: &mut Client, path: &str) -> Status {
    let triple_rpc::Call {
        mut requests,
        response,
    } = client
        .call_stream(path, RpcKind::Bidirectional, &Metadata::new())
        .await
        .unwrap();
    requests.close().unwrap();
    let result = tokio::time::timeout(WAIT, async {
        let mut incoming = response.receive().await?;
        incoming.message::<Echo>().await
    })
    .await
    .unwrap();
    match result {
        Err(triple_rpc::Error::Rpc(status)) => status,
        other => panic!("expected RPC trailers, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_panic_returns_internal_status_not_cancel() {
    let server = Server::start().await;
    let io = tokio::net::TcpStream::connect(&server.address)
        .await
        .unwrap();
    let (mut client, connection) = h2::client::handshake(io).await.unwrap();
    let driver = tokio::spawn(connection);
    let origin = "http://localhost".parse().unwrap();
    for (path, phase) in CASES {
        let request =
            triple_rpc::prepare_request(&origin, path, &Metadata::new(), None, Limits::default())
                .unwrap();
        let (response, _requests) = client.send_request(request, true).unwrap();
        let response = tokio::time::timeout(WAIT, response).await.unwrap().unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(!response.headers().contains_key("grpc-status"));
        let mut body = response.into_body();
        assert!(
            tokio::time::timeout(WAIT, body.data())
                .await
                .unwrap()
                .is_none()
        );
        let trailers = body.trailers().await.unwrap().unwrap();
        println!("panic phase={phase:?} path={path} trailers={trailers:?}");
        assert_eq!(trailers["grpc-status"], "13");
        assert_eq!(trailers["grpc-message"], "handler panicked");
    }
    server.finish().await;
    drop(client);
    tokio::time::timeout(WAIT, driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn handler_panic_is_observable_through_callback() {
    let server = Server::start().await;
    let events = server.events.clone();
    let mut client = server.client().await;
    for (index, (path, phase)) in CASES.into_iter().enumerate() {
        assert_eq!(panic_status(&mut client, path).await.code(), Code::Internal);
        let observed = events.panics.lock().unwrap().clone();
        assert_eq!(observed.len(), index + 1);
        assert_eq!(observed[index], (path.to_owned(), phase));
    }
    server.finish().await;
    assert_eq!(events.panics.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn handler_cancellation_is_not_reported_as_panic() {
    for mode in ["reset", "deadline", "shutdown_abort"] {
        let server = Server::start().await;
        let events = server.events.clone();
        let mut client = server.client().await;
        let options = CallOptions {
            deadline: (mode == "deadline")
                .then(|| Deadline::after(Duration::from_millis(120)).unwrap()),
            ..Default::default()
        };
        let (mut requests, _incoming) = server.held(&mut client, &options).await;
        match mode {
            "reset" => requests.reset(),
            // No client operation is polled after receiving the leading headers:
            // the server's deadline drops the handler, not a client read timeout.
            "deadline" => {}
            // The 40 ms graceful budget expires with an active call, then
            // StreamExecutor aborts its stream future and the handler is dropped.
            "shutdown_abort" => server.stop.cancel(),
            _ => unreachable!(),
        }
        if mode != "shutdown_abort" {
            tokio::time::timeout(WAIT, async {
                while events.active.load(SeqCst) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        server.finish().await;
        assert_eq!(events.dropped.load(SeqCst), 1);
        assert!(events.panics.lock().unwrap().is_empty());
        println!("cancellation mode={mode}: handler_dropped=1 panic_callbacks=0");
    }
}

#[tokio::test]
async fn panic_does_not_leak_payload_to_client() {
    let server = Server::start().await;
    let mut client = server.client().await;
    for (path, _) in CASES {
        let status = panic_status(&mut client, path).await;
        assert_eq!(status.message(), "handler panicked");
        assert!(!format!("{status:?}").contains(SECRET));
        assert!(status.details().is_empty());
    }
    server.finish().await;
}

#[tokio::test]
async fn handler_panic_does_not_affect_sibling_or_connection() {
    let server = Server::start().await;
    let events = server.events.clone();
    let mut client = server.client().await;
    let (_requests, _incoming) = server.held(&mut client, &CallOptions::default()).await;
    for (path, _) in CASES {
        assert_eq!(panic_status(&mut client, path).await.code(), Code::Internal);
        assert_eq!(events.active.load(SeqCst), 1);
        assert_eq!(events.dropped.load(SeqCst), 0);
        let response: Echo = client
            .unary(
                "/test.Panic/Echo",
                &Echo {
                    text: "alive".into(),
                },
                &Metadata::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.text, "alive");
    }
    server.finish().await;
    assert_eq!(events.dropped.load(SeqCst), 1);
    assert_eq!(events.panics.lock().unwrap().len(), 2);
}
