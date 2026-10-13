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

//! Unary connection reuse, metadata and business errors.
#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;
use common::{Echo, spawn_server};
use rpc::{Call, Client, Code, Error, Limits, Metadata};
#[tokio::test]
async fn unary_reuses_connection_and_roundtrips_metadata() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let mut options = Metadata::new();
    options.append_ascii("echo-key", "first").unwrap();
    options.append_ascii("echo-key", "second").unwrap();
    options.append_binary("echo-bin", &[0, 255, 1]).unwrap();
    for text in ["hello", "世界", ""] {
        let Call {
            mut requests,
            response,
        } = client.call("/test.Echo/Unary", &options).await.unwrap();
        requests.send(&Echo { text: text.into() }).await.unwrap();
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
            [vec![0, 255, 1]]
        );
        assert_eq!(
            incoming.message::<Echo>().await.unwrap().unwrap().text,
            text
        );
        assert!(incoming.message::<Echo>().await.unwrap().is_none());
    }
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn unknown_method_and_rich_business_status() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    for (path, code) in [
        ("/test.Echo/unary", Code::Unimplemented),
        ("/test.Echo/Fail", Code::PermissionDenied),
    ] {
        let error = client
            .unary::<_, Echo>(path, &Echo { text: "x".into() }, &Metadata::new())
            .await
            .unwrap_err();
        let Error::Rpc(status) = error else {
            panic!("{error:?}")
        };
        assert_eq!(status.code(), code);
        if code == Code::PermissionDenied {
            assert_eq!(status.message(), "denied: 测试 %");
            assert_eq!(status.details(), &[8, 7]);
        }
    }
    stop.cancel();
    server.await.unwrap().unwrap();
}
