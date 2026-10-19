//! Observes shutdown while the single-port server is still identifying HTTP.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../fake/common/mod.rs"]
mod demo;

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[tokio::test]
async fn pre_cancelled_protocol_detection_returns_ok_without_draining() {
    let server = triple_server::Server::new(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        triple_server::Config {
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    let (io, _peer) = tokio::io::duplex(64);
    let stop = triple_rpc::CancellationToken::new();
    stop.cancel();
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), server.serve_connection(io, stop))
        .await
        .unwrap();
    println!(
        "single-port pre-cancelled: budget_ms=1000 elapsed_ms={:.3} result={result:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(result.is_ok(), "{result:?}");
}

struct InterruptedIo;

impl AsyncRead for InterruptedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "Cancelled",
        )))
    }
}

impl AsyncWrite for InterruptedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn interrupted_without_local_shutdown_still_returns_err() {
    let server = triple_server::Server::new(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        Default::default(),
    );
    let stop = triple_rpc::CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        server.serve_connection(InterruptedIo, stop.clone()),
    )
    .await
    .unwrap();
    println!(
        "single-port interrupted: stop={} result={result:?}",
        stop.is_cancelled()
    );
    assert!(!stop.is_cancelled());
    assert!(matches!(
        result,
        Err(triple_server::Error::Connection(error))
            if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                error.kind() == std::io::ErrorKind::Interrupted
                    && error.to_string() == "Cancelled"
            })
    ));
}

#[tokio::test]
async fn cancellation_after_http2_handshake_still_drains_connection() {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let server = triple_server::Server::new(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        triple_server::Config {
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (io, _) = listener.accept().await.unwrap();
    let stop = triple_rpc::CancellationToken::new();
    let server_task = tokio::spawn({
        let stop = stop.clone();
        async move { server.serve_connection(io, stop).await }
    });
    let (mut client, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(peer))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    // A completed request proves protocol detection and the HTTP/2 handshake finished.
    let request = http::Request::post("http://localhost/demo.Greeter/SayHello")
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from_static(br#"[{"name":"Alice"}]"#)))
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), client.send_request(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    response.into_body().collect().await.unwrap();
    stop.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .unwrap()
        .unwrap();
    println!("single-port after HTTP/2 handshake: result={result:?}");
    assert!(result.is_ok(), "{result:?}");
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
