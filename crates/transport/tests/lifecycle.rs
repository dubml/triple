// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Real loopback HTTP/2 tests independent of RPC dispatch and Protobuf schemas.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use transport::{
    CancellationToken, Client, Error, Limits, Receiver, Result, Sender, serve_connection,
};

fn final_trailers() -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    trailers
}

async fn open(client: &mut Client, path: &str) -> (Sender, h2::client::ResponseFuture) {
    client.ready().await.unwrap();
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://localhost{path}"))
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(())
        .unwrap();
    let (response, stream) = client.send_request(request, false).unwrap();
    (
        Sender::new(stream, Limits::default().max_message_size),
        response,
    )
}

async fn receive(response: h2::client::ResponseFuture) -> Receiver {
    let response = response.await.unwrap();
    assert_eq!(response.status(), 200);
    Receiver::new(response.into_body(), Limits::default().max_message_size)
}

async fn echo(
    request: Request<h2::RecvStream>,
    mut responder: h2::server::SendResponse<Bytes>,
) -> Result<()> {
    let response = Response::builder()
        .header("content-type", "application/grpc+proto")
        .body(())
        .unwrap();
    let mut sender = Sender::new(
        responder.send_response(response, false)?,
        Limits::default().max_message_size,
    );
    let mut receiver = Receiver::new(request.into_body(), Limits::default().max_message_size);
    let reset = sender.reset_signal();
    tokio::select! {
        result = async {
            while let Some(payload) = receiver.message().await? {
                sender.send(&payload).await?;
            }
            sender.finish(final_trailers())
        } => result,
        _ = reset => {
            sender.reset();
            Ok(())
        }
    }
}

async fn echo_server() -> (String, CancellationToken, JoinHandle<Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            |request, responder| async move {
                let _ = echo(request, responder).await;
            },
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    (address, stop, task)
}

async fn join_server(task: JoinHandle<Result<()>>) {
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    match result {
        Ok(()) => {}
        // Dropping the client driver may close TCP with a peer reset.
        Err(Error::Transport(error)) if error.is_io() => {}
        Err(error) => panic!("unexpected server error: {error}"),
    }
}

#[tokio::test]
async fn slow_consumer_applies_backpressure_and_dropped_send_resets_partial_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .initial_window_size(1024)
            .handshake::<_, Bytes>(io)
            .await
            .unwrap();
        let (request, mut responder) = connection.accept().await.unwrap().unwrap();
        let response = Response::builder().body(()).unwrap();
        let _send = responder.send_response(response, false).unwrap();
        // Keep the receive stream alive without consuming or releasing its window.
        let _request = request;
        let _ = connection.accept().await;
    });
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let (mut requests, response) = open(&mut client, "/test/Stream").await;
    let _incoming = receive(response).await;
    let large = vec![b'x'; 200_000];
    assert!(
        tokio::time::timeout(Duration::from_millis(40), requests.send(&large))
            .await
            .is_err()
    );
    assert!(matches!(
        requests.send(b"after-partial-frame").await,
        Err(Error::StreamClosed)
    ));
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn long_bidirectional_stream_preserves_order_and_independent_half_close() {
    let (address, stop, server) = echo_server().await;
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let (mut requests, response) = open(&mut client, "/test/Stream").await;
    let mut incoming = receive(response).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        for number in 0_u32..1024 {
            let message = number.to_be_bytes();
            requests.send(&message).await.unwrap();
            assert_eq!(
                incoming.message().await.unwrap().unwrap(),
                message.as_slice()
            );
        }
    })
    .await
    .unwrap();
    requests.send(&[]).await.unwrap();
    requests.close().unwrap();
    assert!(incoming.message().await.unwrap().unwrap().is_empty());
    assert!(incoming.message().await.unwrap().is_none());
    assert_eq!(incoming.trailers().unwrap()["grpc-status"], "0");
    assert!(incoming.message().await.unwrap().is_none());
    assert!(matches!(requests.close(), Err(Error::StreamClosed)));
    drop(client);
    stop.cancel();
    join_server(server).await;
}

#[tokio::test]
async fn multiplexed_open_stream_does_not_block_another_stream() {
    let (address, stop, server) = echo_server().await;
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let (mut first, response) = open(&mut client, "/test/First").await;
    let mut first_response = receive(response).await;
    first.send(b"held-open").await.unwrap();
    let (mut second, response) = open(&mut client, "/test/Second").await;
    second.send(b"second").await.unwrap();
    second.close().unwrap();
    let mut second_response = receive(response).await;
    assert_eq!(
        second_response.message().await.unwrap().unwrap(),
        b"second".as_slice()
    );
    assert!(second_response.message().await.unwrap().is_none());
    assert_eq!(
        first_response.message().await.unwrap().unwrap(),
        b"held-open".as_slice()
    );
    first.reset();
    assert!(
        matches!(first_response.message().await, Err(Error::Transport(error)) if error.reason() == Some(h2::Reason::CANCEL))
    );
    drop(client);
    stop.cancel();
    join_server(server).await;
}

#[tokio::test]
async fn messages_larger_than_flow_windows_complete_in_both_directions() {
    let (address, stop, server) = echo_server().await;
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let (mut requests, response) = open(&mut client, "/test/Large").await;
    let mut incoming = receive(response).await;
    let payload = vec![b'x'; 200_000];
    tokio::time::timeout(Duration::from_secs(3), async {
        requests.send(&payload).await.unwrap();
        requests.close().unwrap();
        assert_eq!(
            incoming.message().await.unwrap().unwrap(),
            payload.as_slice()
        );
        assert!(incoming.message().await.unwrap().is_none());
    })
    .await
    .unwrap();
    drop(client);
    stop.cancel();
    join_server(server).await;
}

struct HandlerGuard(Arc<AtomicUsize>);
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct TrackedServer {
    address: String,
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
    started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

async fn tracked_server(delay: Duration, shutdown_timeout: Duration) -> TrackedServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let stop = CancellationToken::new();
    let notify = Arc::clone(&started);
    let count = Arc::clone(&dropped);
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            move |request, mut responder| {
                if request.uri().path() == "/test/FactoryPanic" {
                    panic!("intentional handler construction panic");
                }
                let notify = Arc::clone(&notify);
                let count = Arc::clone(&count);
                async move {
                    if request.uri().path() == "/test/Panic" {
                        panic!("intentional stream handler panic");
                    }
                    let _guard = HandlerGuard(count);
                    let stream = responder
                        .send_response(Response::builder().body(()).unwrap(), false)
                        .unwrap();
                    let mut sender = Sender::new(stream, Limits::default().max_message_size);
                    let reset = sender.reset_signal();
                    let mut incoming =
                        Receiver::new(request.into_body(), Limits::default().max_message_size);
                    tokio::select! {
                        _ = async {
                            let payload = incoming.message().await.unwrap().unwrap();
                            notify.notify_one();
                            tokio::time::sleep(delay).await;
                            sender.send(&payload).await.unwrap();
                            let mut trailers = final_trailers();
                            trailers.insert("completed", HeaderValue::from_static("yes"));
                            sender.finish(trailers).unwrap();
                        } => {},
                        _ = reset => { sender.reset(); },
                    }
                }
            },
            Limits::default(),
            shutdown,
            shutdown_timeout,
        )
        .await
    });
    TrackedServer {
        address,
        stop,
        task,
        started,
        dropped,
    }
}

async fn started(server: &TrackedServer) {
    tokio::time::timeout(Duration::from_secs(2), server.started.notified())
        .await
        .unwrap();
}

async fn wait_for_drop(server: &TrackedServer) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_reset_drops_handler_future_and_connection_remains_usable() {
    let server = tracked_server(Duration::from_secs(60), Duration::from_secs(1)).await;
    let mut client = Client::connect(&server.address, Limits::default())
        .await
        .unwrap();
    let (mut requests, response) = open(&mut client, "/test/Held").await;
    let incoming = receive(response).await;
    requests.send(b"cancel").await.unwrap();
    started(&server).await;
    requests.reset();
    drop(incoming);
    wait_for_drop(&server).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    let (mut second, response) = open(&mut client, "/test/Next").await;
    let incoming = receive(response).await;
    second.send(b"still-usable").await.unwrap();
    started(&server).await;
    second.reset();
    drop(incoming);
    drop(client);
    server.stop.cancel();
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn graceful_shutdown_drains_active_stream_and_preserves_trailers() {
    let server = tracked_server(Duration::from_millis(30), Duration::from_secs(1)).await;
    let mut client = Client::connect(&server.address, Limits::default())
        .await
        .unwrap();
    let (mut requests, response) = open(&mut client, "/test/Drained").await;
    requests.send(b"drained").await.unwrap();
    requests.close().unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    server.stop.cancel();
    assert_eq!(
        incoming.message().await.unwrap().unwrap(),
        b"drained".as_slice()
    );
    assert!(incoming.message().await.unwrap().is_none());
    assert_eq!(incoming.trailers().unwrap()["completed"], "yes");
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    // GOAWAY does not admit new streams after the connection has drained.
    assert!(client.ready().await.is_err());
}

#[tokio::test]
async fn shutdown_timeout_aborts_and_joins_active_handlers() {
    let server = tracked_server(Duration::from_secs(60), Duration::from_millis(20)).await;
    let mut client = Client::connect(&server.address, Limits::default())
        .await
        .unwrap();
    let (mut requests, response) = open(&mut client, "/test/Abort").await;
    requests.send(b"abort").await.unwrap();
    requests.close().unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    server.stop.cancel();
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), incoming.message())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn connection_loss_aborts_and_joins_active_handlers() {
    let server = tracked_server(Duration::from_secs(60), Duration::from_secs(1)).await;
    let mut client = Client::connect(&server.address, Limits::default())
        .await
        .unwrap();
    let (mut requests, response) = open(&mut client, "/test/Disconnect").await;
    requests.send(b"disconnect").await.unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    drop(client);
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), incoming.message())
            .await
            .unwrap()
            .is_err()
    );
    assert!(requests.send(b"after-disconnect").await.is_err());
}

#[tokio::test]
async fn handler_panic_is_reported_and_other_handlers_are_joined() {
    for path in ["/test/Panic", "/test/FactoryPanic"] {
        let server = tracked_server(Duration::from_secs(60), Duration::from_secs(1)).await;
        let mut client = Client::connect(&server.address, Limits::default())
            .await
            .unwrap();
        let (mut requests, response) = open(&mut client, "/test/Held").await;
        requests.send(b"held").await.unwrap();
        let _incoming = receive(response).await;
        started(&server).await;
        let (_request, _response) = open(&mut client, path).await;
        let result = tokio::time::timeout(Duration::from_secs(2), server.task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(Error::HandlerTask(error)) if error.is_panic()));
        assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_handshake_and_invalid_limits_do_not_start_connection_tasks() {
    let (io, _peer) = tokio::io::duplex(64);
    let stop = CancellationToken::new();
    stop.cancel();
    tokio::time::timeout(
        Duration::from_secs(1),
        serve_connection(
            io,
            |_request, _response| async {},
            Limits::default(),
            stop,
            Duration::from_secs(1),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let (io, _peer) = tokio::io::duplex(64);
    let limits = Limits {
        max_header_size: usize::MAX,
        ..Limits::default()
    };
    assert!(matches!(
        Client::from_io(io, limits).await,
        Err(Error::InvalidConfig("header limit"))
    ));
}

#[tokio::test]
async fn malformed_frames_from_raw_http2_peer_propagate_once() {
    for wire in [
        Bytes::from_static(&[0, 0, 0, 0, 3, 1]),
        Bytes::from_static(&[0, 255, 255, 255, 255]),
        Bytes::from_static(&[2, 0, 0, 0, 0]),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(io).await.unwrap();
            let (_request, mut response) = connection.accept().await.unwrap().unwrap();
            let mut stream = response
                .send_response(Response::builder().body(()).unwrap(), false)
                .unwrap();
            stream.send_data(wire, true).unwrap();
            let _ = connection.accept().await;
        });
        let mut client = Client::connect(&address, Limits::default()).await.unwrap();
        let (_request, response) = open(&mut client, "/test/BadFrame").await;
        let mut incoming = receive(response).await;
        assert!(matches!(
            incoming.message().await,
            Err(Error::Frame(
                codec::Error::TruncatedFrame
                    | codec::Error::LimitExceeded { .. }
                    | codec::Error::InvalidCompressionFlag(2)
            ))
        ));
        assert!(incoming.message().await.unwrap().is_none());
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
