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

//! Server assertions migrated from transport's lifecycle tests without weaker bounds.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use http::Request;
use rpc::{
    CancellationToken, Incoming, Limits, Metadata, Outgoing, Result, RpcKind, SendRole, Service,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use transport::Client;

// Only the payload adapter changes: rpc's public handler accepts Protobuf messages.
// Retain transport::Client so the original post-GOAWAY ready() assertion survives.
#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}
fn payload(data: &[u8]) -> Payload {
    Payload {
        data: data.to_vec(),
    }
}

struct HandlerGuard(Arc<AtomicUsize>);
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct TrackedService {
    started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    delay: Duration,
}
impl Service for TrackedService {
    fn contains(&self, _path: &str) -> bool {
        true
    }
    fn kind(&self, _path: &str) -> Option<RpcKind> {
        Some(RpcKind::Bidirectional)
    }
    async fn call(
        &self,
        _path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> Result<()> {
        let _guard = HandlerGuard(self.dropped.clone());
        let message = request.message::<Payload>().await?.unwrap();
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        response.send(&message).await?;
        response.trailers_mut().append_ascii("completed", "yes")?;
        Ok(())
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
    let shutdown = stop.clone();
    let service = Arc::new(TrackedService {
        started: started.clone(),
        dropped: dropped.clone(),
        delay,
    });
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        rpc::serve_connection(io, service, Limits::default(), shutdown, shutdown_timeout).await
    });
    TrackedServer {
        address,
        stop,
        task,
        started,
        dropped,
    }
}
async fn open(client: &mut Client, path: &str) -> (Outgoing, h2::client::ResponseFuture) {
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
        Outgoing::new(stream, Limits::default(), false, SendRole::Request),
        response,
    )
}
async fn receive(response: h2::client::ResponseFuture) -> Incoming {
    let response = response.await.unwrap();
    assert_eq!(response.status(), 200);
    Incoming::new(
        response.into_body(),
        Metadata::new(),
        Limits::default(),
        false,
        true,
    )
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
async fn join_server(task: JoinHandle<Result<()>>) {
    // rpc's approved connection-result policy makes peer disconnects successful.
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
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
    requests.send(&payload(b"cancel")).await.unwrap();
    started(&server).await;
    requests.reset();
    drop(incoming);
    wait_for_drop(&server).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    let (mut second, response) = open(&mut client, "/test/Next").await;
    let incoming = receive(response).await;
    second.send(&payload(b"still-usable")).await.unwrap();
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
    requests.send(&payload(b"drained")).await.unwrap();
    requests.close().unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    server.stop.cancel();
    assert_eq!(
        incoming
            .message::<Payload>()
            .await
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"drained".as_slice()
    );
    assert!(incoming.message::<Payload>().await.unwrap().is_none());
    assert_eq!(
        incoming
            .trailers()
            .ascii_values("completed")
            .collect::<Vec<_>>(),
        ["yes"]
    );
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
    requests.send(&payload(b"abort")).await.unwrap();
    requests.close().unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    server.stop.cancel();
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), incoming.message::<Payload>())
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
    requests.send(&payload(b"disconnect")).await.unwrap();
    let mut incoming = receive(response).await;
    started(&server).await;
    drop(client);
    join_server(server.task).await;
    assert_eq!(server.dropped.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), incoming.message::<Payload>())
            .await
            .unwrap()
            .is_err()
    );
    assert!(requests.send(&payload(b"after-disconnect")).await.is_err());
}

#[tokio::test]
async fn cancelled_handshake_does_not_start_connection_tasks() {
    let (io, _peer) = tokio::io::duplex(64);
    let stop = CancellationToken::new();
    stop.cancel();
    tokio::time::timeout(
        Duration::from_secs(1),
        rpc::serve_connection(
            io,
            Arc::new(TrackedService {
                started: Arc::new(Notify::new()),
                dropped: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_secs(60),
            }),
            Limits::default(),
            stop,
            Duration::from_secs(1),
        ),
    )
    .await
    .unwrap()
    .unwrap();
}
