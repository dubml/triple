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

//! Deadline and cancellation isolation on a multiplexed connection.
#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;
use common::{Echo, spawn_server};
use rpc::{Call, CallOptions, Client, Deadline, Error, Limits, RpcKind};
use std::time::Duration;
#[tokio::test]
async fn deadline_and_cancellation_do_not_kill_other_streams() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let options = CallOptions {
        deadline: Some(Deadline::after(Duration::from_millis(40)).unwrap()),
        ..Default::default()
    };
    assert!(matches!(
        client
            .unary_with_options::<_, Echo>("/test.Echo/Slow", &Echo { text: "x".into() }, &options)
            .await,
        Err(Error::DeadlineExceeded) | Err(Error::Rpc(_))
    ));
    let options = CallOptions::default();
    let Call {
        mut requests,
        response,
    } = client
        .call_with_options("/test.Echo/Slow", RpcKind::Unary, &options)
        .await
        .unwrap();
    requests.send(&Echo { text: "x".into() }).await.unwrap();
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    options.cancellation.cancel();
    assert!(matches!(
        incoming.message::<Echo>().await,
        Err(Error::Cancelled)
    ));
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    let echo = client
        .unary_with_options::<_, Echo>(
            "/test.Echo/Unary",
            &Echo {
                text: "alive".into(),
            },
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(echo.text, "alive");
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn server_rejects_malformed_timeouts_and_zero_budget_expires() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let (mut sender, connection) =
        h2::client::handshake(tokio::net::TcpStream::connect(&address).await.unwrap())
            .await
            .unwrap();
    let driver = tokio::spawn(connection);
    for (timeout, expected) in [("100000000S", "3"), ("1x", "3"), ("0n", "4")] {
        std::future::poll_fn(|cx| sender.poll_ready(cx))
            .await
            .unwrap();
        let request = http::Request::builder()
            .method("POST")
            .uri(format!("http://{address}/test.Echo/Slow"))
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-timeout", timeout)
            .body(())
            .unwrap();
        let (response, _) = sender.send_request(request, true).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), response)
            .await
            .unwrap()
            .unwrap();
        let status = if let Some(status) = response.headers().get("grpc-status") {
            status.clone()
        } else {
            let mut body = response.into_body();
            while let Some(data) = body.data().await {
                let data = data.unwrap();
                body.flow_control().release_capacity(data.len()).unwrap();
            }
            body.trailers().await.unwrap().unwrap()["grpc-status"].clone()
        };
        assert_eq!(status, expected);
    }
    stop.cancel();
    server.await.unwrap().unwrap();
    driver.abort();
    let _ = driver.await;
}

#[tokio::test]
async fn pre_cancelled_call_is_rejected_without_affecting_other_calls() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let options = CallOptions::default();
    options.cancellation.cancel();
    assert!(matches!(
        client
            .call_with_options("/test.Echo/Unary", RpcKind::Unary, &options)
            .await,
        Err(Error::Cancelled)
    ));
    assert_eq!(
        client
            .unary_with_options::<_, Echo>(
                "/test.Echo/Unary",
                &Echo {
                    text: "alive".into()
                },
                &CallOptions::default()
            )
            .await
            .unwrap()
            .text,
        "alive"
    );
    stop.cancel();
    server.await.unwrap().unwrap();
}
