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

//! Independent Tonic fixture for Triple wire interoperability tests.

use std::{pin::Pin, time::Duration};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

/// Generated fixture messages, client and server bindings.
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("test");
}

use proto::{EchoMessage, echo_server::Echo};

type EchoStream = Pin<Box<dyn Stream<Item = Result<EchoMessage, Status>> + Send>>;

/// Tonic's native gRPC server; no Triple implementation code is used here.
#[derive(Default)]
pub struct TonicEcho;

#[tonic::async_trait]
impl Echo for TonicEcho {
    type ServerStreamingStream = EchoStream;
    type BidirectionalStream = EchoStream;

    async fn unary(&self, request: Request<EchoMessage>) -> Result<Response<EchoMessage>, Status> {
        let (metadata, _, message) = request.into_parts();
        let mut response = Response::new(message);
        *response.metadata_mut() = metadata;
        Ok(response)
    }

    async fn server_streaming(
        &self,
        request: Request<EchoMessage>,
    ) -> Result<Response<EchoStream>, Status> {
        let message = request.into_inner();
        let messages = ["0", "1", "2"].map(|suffix| {
            Ok(EchoMessage {
                text: format!("{}:{suffix}", message.text),
            })
        });
        Ok(Response::new(Box::pin(tokio_stream::iter(messages))))
    }

    async fn client_streaming(
        &self,
        request: Request<Streaming<EchoMessage>>,
    ) -> Result<Response<EchoMessage>, Status> {
        let mut incoming = request.into_inner();
        let mut text = String::new();
        while let Some(message) = incoming.message().await? {
            text.push_str(&message.text);
        }
        Ok(Response::new(EchoMessage { text }))
    }

    async fn bidirectional(
        &self,
        request: Request<Streaming<EchoMessage>>,
    ) -> Result<Response<EchoStream>, Status> {
        let mut incoming = request.into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                match incoming.message().await {
                    Ok(Some(message)) => {
                        if sender.send(Ok(message)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn fail(&self, _request: Request<EchoMessage>) -> Result<Response<EchoMessage>, Status> {
        Err(Status::with_details(
            tonic::Code::PermissionDenied,
            "denied: 测试 %",
            vec![8, 7].into(),
        ))
    }
    async fn slow(&self, _request: Request<EchoMessage>) -> Result<Response<EchoMessage>, Status> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(Response::new(EchoMessage {
            text: "late".into(),
        }))
    }
}
