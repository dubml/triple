//! Verify a real tonic zero timeout reaches the wire and is parsed as expired.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use rpc::{CancellationToken, Code, Deadline, Error, Limits, Status};
use rpc_interop::proto::{EchoMessage, echo_client::EchoClient};
use tokio::net::TcpListener;

#[tokio::test]
async fn tonic_zero_timeout_reaches_wire_and_native_parser_marks_it_expired() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        transport::serve_connection(
            io,
            move |request, mut responder| {
                let sent = sent.clone();
                async move {
                    let timeout = request.headers()["grpc-timeout"].to_str().unwrap();
                    let error = Deadline::from_header(timeout)
                        .unwrap()
                        .to_header()
                        .unwrap_err();
                    assert!(matches!(error, Error::DeadlineExceeded));
                    sent.send(timeout.to_owned()).unwrap();
                    let _ = rpc::immediate_error(
                        &mut responder,
                        &Status::new(Code::DeadlineExceeded, "expired"),
                        Limits::default(),
                    );
                }
            },
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
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
