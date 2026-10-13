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

//! Bidirectional wire interoperability with independent Tonic client and server.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../../rpc/tests/common/mod.rs"]
mod common;

use common::{Echo, spawn_server};
use rpc::{Call, Client, Code, Error, Limits, Metadata};
use rpc_interop::{
    TonicEcho,
    proto::{EchoMessage, echo_client::EchoClient, echo_server::EchoServer},
};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn native_client_to_independent_tonic_server_unary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, stopping) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(TonicEcho))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopping.await;
            }),
    );
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let mut options = Metadata::new();
    options.append_ascii("echo-key", "first").unwrap();
    options.append_ascii("echo-key", "second").unwrap();
    options.append_binary("echo-bin", &[1, 2, 255]).unwrap();
    let Call {
        mut requests,
        response,
    } = client.call("/test.Echo/Unary", &options).await.unwrap();
    requests
        .send(&Echo {
            text: "unary".into(),
        })
        .await
        .unwrap();
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    assert_eq!(
        incoming
            .metadata()
            .ascii_values("echo-key")
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(
        incoming.metadata().binary_values("echo-bin").unwrap(),
        [vec![1, 2, 255]]
    );
    assert_eq!(
        incoming.message::<Echo>().await.unwrap().unwrap().text,
        "unary"
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    let error = client
        .unary::<_, Echo>("/test.Echo/Fail", &Echo { text: "x".into() }, &options)
        .await
        .unwrap_err();
    let Error::Rpc(status) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "denied: 测试 %");
    assert_eq!(status.details(), &[8, 7]);
    drop(incoming);
    drop(client);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn independent_tonic_client_to_native_server_unary() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = EchoClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(EchoMessage {
        text: "unary".into(),
    });
    request
        .metadata_mut()
        .append("echo-key", "first".parse().unwrap());
    request
        .metadata_mut()
        .append("echo-key", "second".parse().unwrap());
    request.metadata_mut().append_bin(
        "echo-bin",
        tonic::metadata::MetadataValue::from_bytes(&[1, 2, 255]),
    );
    let response = client.unary(request).await.unwrap();
    assert_eq!(
        response
            .metadata()
            .get_all("echo-key")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(
        response
            .metadata()
            .get_bin("echo-bin")
            .unwrap()
            .to_bytes()
            .unwrap()
            .as_ref(),
        &[1, 2, 255]
    );
    assert_eq!(response.into_inner().text, "unary");
    let error = client
        .fail(EchoMessage { text: "x".into() })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(error.message(), "denied: 测试 %");
    assert_eq!(error.details(), &[8, 7]);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(client);
}
