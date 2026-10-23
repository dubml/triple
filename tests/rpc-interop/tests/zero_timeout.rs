//! Verify a real tonic zero timeout reaches the wire and is parsed as expired.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use protos::echo::{EchoMessage, echo_client::EchoClient};
use tokio::net::TcpListener;
use triple_rpc::{CancellationToken, Code, Deadline, Error, Limits, Status};

struct ExpiredService;
impl triple_rpc::Service for ExpiredService {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<triple_rpc::RpcKind> {
        Some(triple_rpc::RpcKind::Unary)
    }
    async fn call(
        &self,
        _: &str,
        _: &mut triple_rpc::Incoming,
        _: &mut triple_rpc::Outgoing,
    ) -> triple_rpc::Result<()> {
        Err(Status::new(Code::DeadlineExceeded, "expired").into())
    }
}

#[tokio::test]
async fn tonic_zero_timeout_reaches_wire_and_native_parser_marks_it_expired() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let tracker = tokio_util::task::TaskTracker::new();
        let tasks = tracker.clone();
        let connection =
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(
                    hyper_util::rt::TokioIo::new(io),
                    hyper::service::service_fn(
                        move |request: hyper::Request<hyper::body::Incoming>| {
                            let sent = sent.clone();
                            let tasks = tasks.clone();
                            async move {
                                let timeout = request.headers()["grpc-timeout"].to_str().unwrap();
                                let error = Deadline::from_header(timeout)
                                    .unwrap()
                                    .to_header()
                                    .unwrap_err();
                                assert!(matches!(error, Error::DeadlineExceeded));
                                sent.send(timeout.to_owned()).unwrap();
                                triple_rpc::handle(
                                    request,
                                    std::sync::Arc::new(ExpiredService),
                                    Limits::default(),
                                    tasks,
                                )
                                .await
                            }
                        },
                    ),
                );
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => result.unwrap(),
            _ = shutdown.cancelled() => {
                connection.as_mut().graceful_shutdown();
                tokio::time::timeout(Duration::from_secs(1), &mut connection).await.unwrap().unwrap();
            }
        }
        tracker.close();
        tracker.wait().await;
    });
    let mut client = EchoClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(EchoMessage {
        text: "zero".into(),
    });
    request.set_timeout(Duration::ZERO);
    assert_eq!(
        request
            .metadata()
            .get("grpc-timeout")
            .unwrap()
            .to_str()
            .unwrap(),
        "0n"
    );
    let error = client.unary(request).await.unwrap_err();
    assert!(matches!(
        error.code(),
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
    ));
    let timeout = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(timeout, "0n");
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    drop(client);
}
