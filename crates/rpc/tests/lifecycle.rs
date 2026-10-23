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

//! Backpressure, cancellation safety, multiplexing and graceful shutdown tests.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::Echo;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify};
use triple_rpc::{
    Call, CallOptions, CancellationToken, Client, Error, Incoming, Limits, Outgoing, Result,
    RpcKind, Service, serve_connection,
};

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
        Some(RpcKind::Unary)
    }

    async fn call(
        &self,
        _path: &str,
        request: &mut Incoming,
        response: &mut Outgoing,
    ) -> Result<()> {
        let _guard = HandlerGuard(Arc::clone(&self.dropped));
        let message = request
            .message::<Echo>()
            .await?
            .ok_or(Error::Cardinality("request"))?;
        self.started.notify_one();
        tokio::time::sleep(self.delay).await;
        response.trailers_mut().append_ascii("completed", "yes")?;
        response.send(&message).await
    }
}

#[tokio::test]
async fn peer_reset_drops_handler_future_and_connection_remains_usable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let service = Arc::new(TrackedService {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
        delay: Duration::from_secs(60),
    });
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            service,
            Limits::default(),
            CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
    });
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call_with_options("/test.Echo/Unary", RpcKind::Unary, &CallOptions::default())
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "cancel".into(),
        })
        .await
        .unwrap();
    let incoming = response.receive().await.unwrap();
    started.notified().await;
    requests.cancel();
    drop(incoming);
    tokio::time::timeout(Duration::from_secs(1), async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn graceful_shutdown_drains_active_call_and_preserves_trailers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let service = Arc::new(TrackedService {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
        delay: Duration::from_millis(30),
    });
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            service,
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call_with_options("/test.Echo/Unary", RpcKind::Unary, &CallOptions::default())
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "drained".into(),
        })
        .await
        .unwrap();
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    started.notified().await;
    stop.cancel();
    assert_eq!(
        incoming.message::<Echo>().await.unwrap().unwrap().text,
        "drained"
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    assert_eq!(
        incoming
            .trailers()
            .ascii_values("completed")
            .collect::<Vec<_>>(),
        ["yes"]
    );
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_timeout_aborts_and_joins_active_handlers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let service = Arc::new(TrackedService {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
        delay: Duration::from_secs(60),
    });
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            service,
            Limits::default(),
            shutdown,
            Duration::from_millis(20),
        )
        .await
    });
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call_with_options("/test.Echo/Unary", RpcKind::Unary, &CallOptions::default())
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "abort".into(),
        })
        .await
        .unwrap();
    requests.close().unwrap();
    let _incoming = response.receive().await.unwrap();
    started.notified().await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remote_deadline_aborts_handler_and_sends_final_status_without_client_timer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(TrackedService {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
        delay: Duration::from_secs(60),
    });
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_connection(
            io,
            service,
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let io = tokio::net::TcpStream::connect(&address).await.unwrap();
    let (mut sender, connection) = h2::client::handshake(io).await.unwrap();
    let driver = tokio::spawn(connection);
    std::future::poll_fn(|cx| sender.poll_ready(cx))
        .await
        .unwrap();
    let request = http::Request::builder()
        .method("POST")
        .uri(format!("http://{address}/test.Echo/Unary"))
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .header("grpc-timeout", "80m")
        .body(())
        .unwrap();
    let (response, mut send) = sender.send_request(request, false).unwrap();
    send.send_data(
        bytes::Bytes::from_static(&[0, 0, 0, 0, 3, 10, 1, b'x']),
        true,
    )
    .unwrap();
    let mut body = response.await.unwrap().into_body();
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    let trailers = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(data) = body.data().await {
            let data = data.unwrap();
            body.flow_control().release_capacity(data.len()).unwrap();
        }
        body.trailers().await.unwrap().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(trailers["grpc-status"], "4");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    stop.cancel();
    server.await.unwrap().unwrap();
    driver.abort();
    let _ = driver.await;
}
