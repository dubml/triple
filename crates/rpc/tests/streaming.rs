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

//! Streaming RPC ordering and independent request half-close.
#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;
use common::{Echo, spawn_server};
use rpc::{Call, Client, Code, Error, Limits, Metadata, RpcKind};
use std::time::Duration;
#[tokio::test]
async fn all_streaming_modes_and_independent_half_close() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let options = Metadata::new();
    let Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Echo/ServerStreaming",
            RpcKind::ServerStreaming,
            &options,
        )
        .await
        .unwrap();
    requests.send(&Echo { text: "hi".into() }).await.unwrap();
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    for expected in ["hi:0", "hi:1", "hi:2"] {
        assert_eq!(
            incoming.message::<Echo>().await.unwrap().unwrap().text,
            expected
        );
    }
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    let Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Echo/ClientStreaming",
            RpcKind::ClientStreaming,
            &options,
        )
        .await
        .unwrap();
    for text in ["a", "b", "c"] {
        requests.send(&Echo { text: text.into() }).await.unwrap();
    }
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    assert_eq!(
        incoming.message::<Echo>().await.unwrap().unwrap().text,
        "abc"
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    let Call {
        mut requests,
        response,
    } = client
        .call_stream("/test.Echo/Bidirectional", RpcKind::Bidirectional, &options)
        .await
        .unwrap();
    let mut incoming = response.receive().await.unwrap();
    for text in ["first", "second"] {
        requests.send(&Echo { text: text.into() }).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), incoming.message::<Echo>())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .text,
            text
        );
    }
    requests.close().unwrap();
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_streams_are_valid_and_single_response_methods_still_require_one() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    for (path, kind) in [
        ("/test.Echo/Bidirectional", RpcKind::Bidirectional),
        ("/test.Echo/EmptyServerStream", RpcKind::ServerStreaming),
        ("/test.Echo/ClientStreaming", RpcKind::ClientStreaming),
    ] {
        let Call {
            mut requests,
            response,
        } = client
            .call_stream(path, kind, &Metadata::new())
            .await
            .unwrap();
        if kind.single_request() {
            assert!(matches!(requests.close(), Err(Error::Cardinality(_))));
            requests.send(&Echo { text: "one".into() }).await.unwrap();
            assert!(matches!(
                requests
                    .send(&Echo {
                        text: "extra".into()
                    })
                    .await,
                Err(Error::Cardinality(_))
            ));
        }
        requests.close().unwrap();
        assert!(matches!(
            requests.close(),
            Err(Error::Transport(transport::Error::StreamClosed))
        ));
        let mut incoming = response.receive().await.unwrap();
        if kind.single_response() {
            assert_eq!(incoming.message::<Echo>().await.unwrap().unwrap().text, "");
        }
        assert!(incoming.message::<Echo>().await.unwrap().is_none());
        assert!(incoming.message::<Echo>().await.unwrap().is_none());
    }
    for path in [
        "/test.Echo/EmptyClientResponse",
        "/test.Echo/ExtraClientResponse",
    ] {
        let Call {
            mut requests,
            response,
        } = client
            .call_stream(path, RpcKind::ClientStreaming, &Metadata::new())
            .await
            .unwrap();
        requests.close().unwrap();
        let mut incoming = response.receive().await.unwrap();
        if path.ends_with("ExtraClientResponse") {
            assert_eq!(
                incoming.message::<Echo>().await.unwrap().unwrap().text,
                "first"
            );
        }
        assert!(
            matches!(incoming.message::<Echo>().await, Err(Error::Rpc(status)) if status.code() == Code::Internal)
        );
        assert!(incoming.message::<Echo>().await.unwrap().is_none());
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn streamed_business_error_and_trailers_are_delivered_once_before_request_close() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Echo/StreamFail",
            RpcKind::Bidirectional,
            &Metadata::new(),
        )
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "first".into(),
        })
        .await
        .unwrap();
    let mut incoming = response.receive().await.unwrap();
    assert_eq!(
        incoming.message::<Echo>().await.unwrap().unwrap().text,
        "first"
    );
    let error = tokio::time::timeout(Duration::from_secs(2), incoming.message::<Echo>())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, Error::Rpc(status) if status.code() == Code::PermissionDenied && status.message() == "stream failed")
    );
    assert_eq!(
        incoming
            .trailers()
            .ascii_values("failure-key")
            .collect::<Vec<_>>(),
        ["last"]
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    requests.reset();
    let echo = client
        .unary::<_, Echo>(
            "/test.Echo/Unary",
            &Echo {
                text: "alive".into(),
            },
            &Metadata::new(),
        )
        .await
        .unwrap();
    assert_eq!(echo.text, "alive");
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn slow_response_consumer_backpressures_long_stream_and_preserves_order() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call_stream(
            "/test.Echo/Bidirectional",
            RpcKind::Bidirectional,
            &Metadata::new(),
        )
        .await
        .unwrap();
    let (started, starting) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    let mut sender = tokio::spawn(async move {
        for index in 0..16 {
            requests
                .send(&Echo {
                    text: format!("{index}:{}", "x".repeat(200_000)),
                })
                .await
                .unwrap();
            if index == 0 {
                let _ = started.take().unwrap().send(());
            }
        }
        requests.close().unwrap();
    });
    let mut incoming = response.receive().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), starting)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut sender)
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        for index in 0..16 {
            let message = incoming.message::<Echo>().await.unwrap().unwrap();
            assert_eq!(message.text, format!("{index}:{}", "x".repeat(200_000)));
        }
        assert!(incoming.message::<Echo>().await.unwrap().is_none());
        sender.await.unwrap();
    })
    .await
    .unwrap();
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn peer_reset_drops_streaming_handler_and_leaves_other_calls_usable() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Tracked {
        entered: Arc<tokio::sync::Notify>,
        dropped: Arc<AtomicUsize>,
    }
    impl rpc::Service for Tracked {
        fn contains(&self, _path: &str) -> bool {
            true
        }
        fn kind(&self, path: &str) -> Option<RpcKind> {
            Some(if path.ends_with("Unary") {
                RpcKind::Unary
            } else {
                RpcKind::Bidirectional
            })
        }
        async fn call(
            &self,
            path: &str,
            request: &mut rpc::Incoming,
            response: &mut rpc::Outgoing,
        ) -> rpc::Result<()> {
            let _guard = Guard(Arc::clone(&self.dropped));
            let message = request
                .message::<Echo>()
                .await?
                .ok_or(Error::Cardinality("request"))?;
            if path.ends_with("Unary") {
                return response.send(&message).await;
            }
            self.entered.notify_one();
            std::future::pending().await
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(Tracked {
        entered: Arc::clone(&entered),
        dropped: Arc::clone(&dropped),
    });
    let stop = rpc::CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        rpc::serve_connection(
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
        .call_stream(
            "/test.Echo/Bidirectional",
            RpcKind::Bidirectional,
            &Metadata::new(),
        )
        .await
        .unwrap();
    requests
        .send(&Echo {
            text: "cancel".into(),
        })
        .await
        .unwrap();
    let incoming = response.receive().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    requests.reset();
    drop(incoming);
    tokio::time::timeout(Duration::from_secs(2), async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        client
            .unary::<_, Echo>(
                "/test.Echo/Unary",
                &Echo {
                    text: "alive".into()
                },
                &Metadata::new()
            )
            .await
            .unwrap()
            .text,
        "alive"
    );
    stop.cancel();
    server.await.unwrap().unwrap();
}
