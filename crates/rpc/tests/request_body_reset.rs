//! Freezes the hyper request-body error variant after a peer resets partial DATA.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use rpc::{CancellationToken, Incoming, Limits, Outgoing, RpcKind, Service};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
};

#[derive(Clone, PartialEq, prost::Message)]
struct Message {
    #[prost(string, tag = "1")]
    text: String,
}
struct Reader {
    started: Notify,
    errors: mpsc::Sender<rpc::Error>,
}
impl Service for Reader {
    fn contains(&self, _: &str) -> bool {
        true
    }
    fn kind(&self, _: &str) -> Option<RpcKind> {
        Some(RpcKind::ServerStreaming)
    }
    async fn call(&self, _: &str, request: &mut Incoming, _: &mut Outgoing) -> rpc::Result<()> {
        self.started.notify_one();
        let error = request.message::<Message>().await.unwrap_err();
        self.errors.send(error).await.unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn request_body_reset_surfaces_as_io_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, mut received) = mpsc::channel(1);
    let service = Arc::new(Reader {
        started: Notify::new(),
        errors: sent,
    });
    let handler = service.clone();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let (bodies, mut held_body) = mpsc::channel(1);
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let tracker = tokio_util::task::TaskTracker::new();
        let calls = tracker.clone();
        let connection = hyper::server::conn::http2::Builder::new(rpc::StreamExecutor::new(
            tracker.clone(),
            shutdown.clone(),
        ))
        .serve_connection(
            hyper_util::rt::TokioIo::new(io),
            hyper::service::service_fn(move |request| {
                let (handler, calls, bodies) = (handler.clone(), calls.clone(), bodies.clone());
                async move {
                    let response = rpc::handle(request, handler, Limits::default(), calls)
                        .await
                        .unwrap();
                    // Keep the RPC body alive so response cancellation cannot win
                    // the race against observing the request's read error.
                    bodies.send(response.into_body()).await.unwrap();
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(
                        http_body_util::Full::new(bytes::Bytes::new()),
                    ))
                }
            }),
        );
        tokio::pin!(connection);
        tokio::select! { result = &mut connection => result.unwrap(), _ = shutdown.cancelled() => {} }
        shutdown.cancel();
        tracker.close();
        tracker.wait().await;
    });
    let mut io = TcpStream::connect(address).await.unwrap();
    io.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    io.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
    let mut header = [0; 9];
    io.read_exact(&mut header).await.unwrap();
    let length =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    io.read_exact(&mut vec![0; length]).await.unwrap();
    let mut block = vec![0x83, 0x86, 0x04, 16];
    block.extend_from_slice(b"/test.Body/Reset");
    // The path has 16 bytes, and fields are literal HPACK strings without Huffman.
    block.extend_from_slice(&[0, 12]);
    block.extend_from_slice(b"content-type");
    block.push(16);
    block.extend_from_slice(b"application/grpc");
    block.extend_from_slice(&[0, 2, b't', b'e', 8]);
    block.extend_from_slice(b"trailers");
    let mut headers = vec![0, 0, u8::try_from(block.len()).unwrap(), 1, 4, 0, 0, 0, 1];
    headers.extend(block);
    io.write_all(&headers).await.unwrap();
    // gRPC frame announces ten bytes but only three payload bytes arrive.
    io.write_all(&[0, 0, 8, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 10, 10, 1, b'x'])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), service.started.notified())
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(2), held_body.recv())
        .await
        .unwrap()
        .unwrap();
    io.write_all(&[0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8])
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    println!("request body reset: {error:?}");
    assert!(matches!(error, rpc::Error::Io(_)));
    drop(body);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
