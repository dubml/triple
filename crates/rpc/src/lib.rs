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
//! Deadlines, explicit cancellation, TLS/mTLS and graceful shutdown are supported.
//!
//! With encoded frame length `F` and stream window `W = 65535`, successful
//! handler sends not yet admitted by the peer window obey
//! `unsent = max(0, completed * F - W) <= 3 * F`: at most one frame each in h2,
//! hyper's send pipeline and the bounded channel. These queues stay bounded
//! while the consumer stalls. Payload storage per stream is bounded by
//! `W + 3 * max_message_size` (plus framing overhead); a pending handler send
//! separately owns its message and encoded frame until it completes or drops.
//! At the default 128 concurrent streams and 4 MiB message limit, this in-flight
//! bound is 128 * (65535 + 3 * 4 MiB) = 1,619,001,216 bytes; the former server
//! retained approximately one message per stream, or 536,870,912 bytes total.
//! This is a design tradeoff: lower `max_message_size` or `max_concurrent_streams`
//! to tighten it, without counting pending handler messages or other overhead.

mod backend;
mod client;
mod context;
mod deadline;
mod error;
mod http2_settings;
mod hyper_server;
mod kernel;
mod message;
mod server;
mod stream;
mod tls;

pub use client::{
    Call, CallOptions, Client, PendingResponse, ResponseHead, parse_origin, parse_response,
    prepare_request,
};
pub use deadline::Deadline;
pub use error::{Error, Result};
pub use http2_settings::{Http2Settings, http2_settings};
pub use hyper_server::StreamExecutor;
pub use kernel::{
    CallError, Reader, Service, Writer, finish_call, invoke_service, resolve_method, unary,
};
pub use message::{decode_message, encode_message};
pub use server::{ResponseBody, handle, immediate_error, serve_connection};
pub use stream::{Incoming, Outgoing, RpcKind, SendRole};
pub use triple_headers::{Code, Metadata, Status};
pub use triple_transport::{CancellationToken, Limits};

pub use tls::{Identity, client_tls_config, serve_tls_connection, server_tls_config};
pub use tokio_rustls::rustls;
