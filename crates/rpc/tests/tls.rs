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

//! Certificate verification and authenticated HTTP/2 transport tests.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Echo, EchoService};
use rpc::{
    CallOptions, CancellationToken, Client, Identity, Limits, client_tls_config,
    rustls::{RootCertStore, pki_types::PrivatePkcs8KeyDer},
    serve_tls_connection, server_tls_config,
};
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;

fn credentials() -> (Identity, RootCertStore) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let identity = Identity {
        certificates: vec![certified.cert.der().clone()],
        private_key: PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()).into(),
    };
    (identity, roots)
}

#[tokio::test]
async fn verified_tls_roundtrip() {
    let (identity, roots) = credentials();
    let config = server_tls_config(identity, None).unwrap();
    let client_config = client_tls_config(roots, None).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_tls_connection(
            io,
            config,
            Arc::new(EchoService),
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let mut client = Client::connect_tls(&address, "localhost", client_config, Limits::default())
        .await
        .unwrap();
    let echo = client
        .unary_with_options::<_, Echo>(
            "/test.Echo/Unary",
            &Echo { text: "TLS".into() },
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(echo.text, "TLS");
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(client);
}

#[tokio::test]
async fn wrong_ca_and_server_name_are_rejected() {
    for wrong_name in [false, true] {
        let (identity, roots) = credentials();
        let (_, unrelated_roots) = credentials();
        let config = server_tls_config(identity, None).unwrap();
        let client_config =
            client_tls_config(if wrong_name { roots } else { unrelated_roots }, None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            serve_tls_connection(
                io,
                config,
                Arc::new(EchoService),
                Limits::default(),
                CancellationToken::new(),
                Duration::from_secs(1),
            )
            .await
        });
        assert!(
            Client::connect_tls(
                &address,
                if wrong_name {
                    "wrong.example"
                } else {
                    "localhost"
                },
                client_config,
                Limits::default()
            )
            .await
            .is_err()
        );
        assert!(server.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn mtls_requires_a_trusted_client_certificate() {
    for send_identity in [false, true] {
        let (server_identity, server_roots) = credentials();
        let (client_identity, client_roots) = credentials();
        let config = server_tls_config(server_identity, Some(client_roots)).unwrap();
        let client_config =
            client_tls_config(server_roots, send_identity.then_some(client_identity)).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            serve_tls_connection(
                io,
                config,
                Arc::new(EchoService),
                Limits::default(),
                shutdown,
                Duration::from_secs(1),
            )
            .await
        });
        let client =
            Client::connect_tls(&address, "localhost", client_config, Limits::default()).await;
        if send_identity {
            let mut client = client.unwrap();
            assert_eq!(
                client
                    .unary_with_options::<_, Echo>(
                        "/test.Echo/Unary",
                        &Echo {
                            text: "mTLS".into()
                        },
                        &CallOptions::default()
                    )
                    .await
                    .unwrap()
                    .text,
                "mTLS"
            );
            stop.cancel();
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(client);
        } else {
            // TLS 1.3 may report client-auth rejection at the next I/O operation.
            if let Ok(mut client) = client {
                assert!(
                    client
                        .unary_with_options::<_, Echo>(
                            "/test.Echo/Unary",
                            &Echo {
                                text: "rejected".into()
                            },
                            &CallOptions::default()
                        )
                        .await
                        .is_err()
                );
            }
            assert!(server.await.unwrap().is_err());
        }
    }
}

#[tokio::test]
async fn mtls_rejects_untrusted_client_certificate() {
    let (server_identity, server_roots) = credentials();
    let (_, trusted_client_roots) = credentials();
    let (wrong_client_identity, _) = credentials();
    let config = server_tls_config(server_identity, Some(trusted_client_roots)).unwrap();
    let client_config = client_tls_config(server_roots, Some(wrong_client_identity)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_tls_connection(
            io,
            config,
            Arc::new(EchoService),
            Limits::default(),
            CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
    });
    if let Ok(mut client) =
        Client::connect_tls(&address, "localhost", client_config, Limits::default()).await
    {
        assert!(
            client
                .unary_with_options::<_, Echo>(
                    "/test.Echo/Unary",
                    &Echo {
                        text: "rejected".into()
                    },
                    &CallOptions::default()
                )
                .await
                .is_err()
        );
    }
    assert!(server.await.unwrap().is_err());
}

#[tokio::test]
async fn tls_requires_negotiated_h2() {
    let (identity, roots) = credentials();
    let config = server_tls_config(identity, None).unwrap();
    let mut client_config = Arc::try_unwrap(client_tls_config(roots, None).unwrap()).unwrap();
    client_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        serve_tls_connection(
            io,
            config,
            Arc::new(EchoService),
            Limits::default(),
            CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
    });
    assert!(
        Client::connect_tls(
            &address,
            "localhost",
            Arc::new(client_config),
            Limits::default()
        )
        .await
        .is_err()
    );
    assert!(server.await.unwrap().is_err());
}

#[tokio::test]
async fn shutdown_interrupts_incomplete_tls_handshake() {
    let (identity, _) = credentials();
    let config = server_tls_config(identity, None).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let accepted = Arc::new(tokio::sync::Notify::new());
    let ready = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        ready.notify_one();
        serve_tls_connection(
            io,
            config,
            Arc::new(EchoService),
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let peer = tokio::net::TcpStream::connect(&address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), accepted.notified())
        .await
        .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(peer);
}
