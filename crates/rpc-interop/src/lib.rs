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

use tonic::{Request, Response, Status};

/// Generated fixture messages, client and server bindings.
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("test");
}

use proto::{EchoMessage, echo_server::Echo};

/// Tonic's native gRPC server; no Triple implementation code is used here.
#[derive(Default)]
pub struct TonicEcho;

#[tonic::async_trait]
impl Echo for TonicEcho {
    async fn unary(&self, request: Request<EchoMessage>) -> Result<Response<EchoMessage>, Status> {
        let (metadata, _, message) = request.into_parts();
        let mut response = Response::new(message);
        *response.metadata_mut() = metadata;
        Ok(response)
    }

    async fn fail(&self, _request: Request<EchoMessage>) -> Result<Response<EchoMessage>, Status> {
        Err(Status::with_details(
            tonic::Code::PermissionDenied,
            "denied: 测试 %",
            vec![8, 7].into(),
        ))
    }
}
