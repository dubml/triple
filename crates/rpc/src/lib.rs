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

//! Native Protobuf RPC dispatch and calls over the Triple HTTP/2 transport.
//!
//! Unary and all three streaming modes share a bounded message state machine.
//! Deadline enforcement and TLS configuration remain
//! outside this crate at this migration stage.

mod client;
mod error;
mod kernel;
mod message;
mod server;
mod stream;

pub use client::{
    Call, Client, PendingResponse, ResponseHead, parse_origin, parse_response, prepare_request,
};
pub use error::{Error, Result};
pub use headers::{Code, Metadata, Status};
pub use kernel::{
    CallError, Reader, Service, Writer, finish_call, invoke_service, resolve_method, unary,
};
pub use message::{decode_message, encode_message};
pub use server::{immediate_error, serve_connection};
pub use stream::{Incoming, Outgoing, RpcKind, SendRole};
pub use transport::{CancellationToken, Limits};
