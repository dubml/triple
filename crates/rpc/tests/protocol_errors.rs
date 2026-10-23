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

//! Negative wire cases from independently constructed HTTP/2 messages.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use bytes::Bytes;
use common::{Echo, spawn_server};
use http::{Request, Response};
use std::{future::poll_fn, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use triple_rpc::{Call, Client, Code, Error, Limits, Metadata};

struct BadResponseService;

impl triple_rpc::Service for BadResponseService {
    fn contains(&self, _path: &str) -> bool {
        true
    }

    async fn call(
        &self,
        path: &str,
        request: &mut triple_rpc::Incoming,
        response: &mut triple_rpc::Outgoing,
    ) -> triple_rpc::Result<()> {
        let message = request
            .message::<Echo>()
            .await?
            .ok_or(Error::Cardinality("request"))?;
        if path.ends_with("/Extra") {
            response.send(&message).await?;
            response.send(&message).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn server_reports_missing_and_excess_unary_response_as_internal_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let stop = triple_rpc::CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        triple_rpc::serve_connection(
            io,
            std::sync::Arc::new(BadResponseService),
            Limits::default(),
            shutdown,
            Duration::from_secs(1),
        )
        .await
    });
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    for path in ["/test.Echo/Empty", "/test.Echo/Extra"] {
        let error = client
            .unary::<_, Echo>(path, &Echo { text: "x".into() }, &Metadata::new())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Rpc(status) if status.code() == Code::Internal));
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

struct RawClient {
    sender: h2::client::SendRequest<Bytes>,
    driver: JoinHandle<std::result::Result<(), h2::Error>>,
}

impl Drop for RawClient {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

async fn raw_client(address: &str) -> RawClient {
    let (sender, connection) = h2::client::handshake(TcpStream::connect(address).await.unwrap())
        .await
        .unwrap();
    RawClient {
        sender,
        driver: tokio::spawn(connection),
    }
}

#[tokio::test]
async fn bad_request_frames_and_cardinality_return_final_rpc_errors() {
    let limits = Limits {
        max_message_size: 8,
        ..Default::default()
    };
    let (address, stop, server) = spawn_server(limits).await.unwrap();
    let mut client = raw_client(&address).await;
    for method in ["Unary", "ServerStreaming"] {
        for (wire, expected) in [
            (vec![], "3"),
            (vec![0, 0, 0, 0, 1, 255], "3"),
            (vec![1, 0, 0, 0, 0], "12"),
            (vec![2, 0, 0, 0, 0], "13"),
            (vec![0, 0, 0, 0, 9], "8"),
            (vec![0, 0, 0, 0, 2, 10], "13"),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "3"),
        ] {
            poll_fn(|cx| client.sender.poll_ready(cx)).await.unwrap();
            let request = Request::builder()
                .method("POST")
                .uri(format!("http://{address}/test.Echo/{method}"))
                .header("content-type", "application/grpc+proto")
                .header("te", "trailers")
                .body(())
                .unwrap();
            let (response, mut send) = client.sender.send_request(request, false).unwrap();
            send.send_data(Bytes::from(wire), true).unwrap();
            let mut response = response.await.unwrap().into_body();
            while let Some(data) = response.data().await {
                let data = data.unwrap();
                response
                    .flow_control()
                    .release_capacity(data.len())
                    .unwrap();
            }
            let trailers = response.trailers().await.unwrap().unwrap();
            assert_eq!(trailers["grpc-status"], expected);
        }
    }
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_method_content_type_query_and_unknown_method_are_rejected() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = raw_client(&address).await;
    for (method, path, media, http_status, rpc_status) in [
        (
            "GET",
            "/test.Echo/Unary",
            "application/grpc",
            200,
            Some("3"),
        ),
        ("POST", "/test.Echo/Unary", "text/plain", 415, None),
        (
            "POST",
            "/test.Echo/Unary?x=y",
            "application/grpc",
            200,
            Some("3"),
        ),
        (
            "POST",
            "/test.Echo/unknown",
            "application/grpc",
            200,
            Some("12"),
        ),
    ] {
        poll_fn(|cx| client.sender.poll_ready(cx)).await.unwrap();
        let request = Request::builder()
            .method(method)
            .uri(format!("http://{address}{path}"))
            .header("content-type", media)
            .header("te", "trailers")
            .body(())
            .unwrap();
        let (response, _) = client.sender.send_request(request, true).unwrap();
        let response = response.await.unwrap();
        assert_eq!(response.status().as_u16(), http_status);
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            rpc_status
        );
        assert!(response.body().is_end_stream());
    }
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}

async fn raw_response(
    status: u16,
    media: &str,
    wire: &'static [u8],
    trailing_status: Option<&str>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let media = media.to_owned();
    let trailing_status = trailing_status.map(str::to_owned);
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        let (mut request, mut response) = connection.accept().await.unwrap().unwrap();
        let handler = tokio::spawn(async move {
            while let Some(data) = request.body_mut().data().await {
                let data = data.unwrap();
                request
                    .body_mut()
                    .flow_control()
                    .release_capacity(data.len())
                    .unwrap();
            }
            let response_headers = Response::builder()
                .status(status)
                .header("content-type", media)
                .body(())
                .unwrap();
            let mut stream = response.send_response(response_headers, false).unwrap();
            stream
                .send_data(Bytes::from_static(wire), trailing_status.is_none())
                .unwrap();
            if let Some(status) = trailing_status {
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", status.parse().unwrap());
                stream.send_trailers(trailers).unwrap();
            }
        });
        let _ = connection.accept().await;
        handler.await.unwrap();
    });
    (address, server)
}

#[tokio::test]
async fn client_rejects_missing_status_truncation_and_duplicate_messages() {
    for (wire, trailing_status) in [
        (&[0, 0, 0, 0, 0][..], None),
        (&[0, 0, 0, 0, 2, 10][..], Some("0")),
        (&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0][..], Some("0")),
        (&[][..], Some("0")),
    ] {
        let (address, server) =
            raw_response(200, "application/grpc+proto", wire, trailing_status).await;
        let mut client = Client::connect(&address, Limits::default()).await.unwrap();
        let error = client
            .unary::<_, Echo>(
                "/test.Echo/Unary",
                &Echo { text: "x".into() },
                &Metadata::new(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Headers(triple_headers::Error::MissingStatus)
                    | Error::Transport(triple_transport::Error::Frame(
                        triple_codec::Error::TruncatedFrame
                    ))
                    | Error::Cardinality(_)
            ),
            "{error:?}"
        );
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn client_maps_http_errors_and_rejects_non_protobuf_content() {
    for (status, media) in [(404, "text/plain"), (200, "application/grpc+json")] {
        let (address, server) = raw_response(status, media, &[], None).await;
        let mut client = Client::connect(&address, Limits::default()).await.unwrap();
        let error = client
            .unary::<_, Echo>(
                "/test.Echo/Unary",
                &Echo { text: "x".into() },
                &Metadata::new(),
            )
            .await
            .unwrap_err();
        if status == 404 {
            assert!(matches!(error, Error::Rpc(status) if status.code() == Code::Unimplemented));
        } else {
            assert!(matches!(
                error,
                Error::Headers(triple_headers::Error::InvalidHeader("content-type"))
            ));
        }
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn client_checks_request_cardinality_before_half_close() {
    let (address, stop, server) = spawn_server(Limits::default()).await.unwrap();
    let mut client = Client::connect(&address, Limits::default()).await.unwrap();
    let Call {
        mut requests,
        response,
    } = client
        .call("/test.Echo/Unary", &Metadata::new())
        .await
        .unwrap();
    assert!(matches!(requests.close(), Err(Error::Cardinality(_))));
    requests.send(&Echo { text: "one".into() }).await.unwrap();
    assert!(matches!(
        requests.send(&Echo { text: "two".into() }).await,
        Err(Error::Cardinality(_))
    ));
    requests.close().unwrap();
    let mut incoming = response.receive().await.unwrap();
    assert_eq!(
        incoming.message::<Echo>().await.unwrap().unwrap().text,
        "one"
    );
    assert!(incoming.message::<Echo>().await.unwrap().is_none());
    drop(client);
    stop.cancel();
    server.await.unwrap().unwrap();
}
