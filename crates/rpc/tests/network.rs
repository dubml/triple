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

//! Real HTTP/2 RPC lifecycle and flow-control tests.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Echo, spawn_server};
use std::time::Duration;
use triple_rpc::{Call, CallOptions, Client, Code, Error, Limits, RpcKind};

#[tokio::test]
async fn errors_unknown_method_and_rich_status() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    for (path, expected) in [
        ("/test.Echo/Fail", Code::PermissionDenied),
        ("/test.Echo/Missing", Code::Unimplemented),
    ] {
        let error = client
            .unary_with_options::<_, Echo>(
                path,
                &Echo { text: "x".into() },
                &CallOptions::default(),
            )
            .await
            .unwrap_err();
        let Error::Rpc(status) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(status.code(), expected);
        if expected == Code::PermissionDenied {
            assert_eq!(status.message(), "denied: 测试 %");
            assert_eq!(status.details(), &[8, 7]);
        }
    }
    let options = CallOptions::default();
    let Call { requests, response } = client
        .call_with_options("/test.Echo/Missing", RpcKind::Unary, &options)
        .await
        .unwrap();
    // The final trailers-only status has already arrived; no further transport wait is needed.
    let mut incoming = response.receive().await.unwrap();
    options.cancellation.cancel();
    assert!(
        matches!(incoming.message::<Echo>().await, Err(triple_rpc::Error::Rpc(status)) if status.code() == Code::Unimplemented)
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    drop(requests);
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn messages_larger_than_flow_window_do_not_deadlock() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let message = Echo {
        text: "x".repeat(200_000),
    };
    let echo = tokio::time::timeout(
        Duration::from_secs(2),
        client.unary_with_options::<_, Echo>("/test.Echo/Unary", &message, &CallOptions::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(echo, message);
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}
