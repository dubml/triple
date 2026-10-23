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

//! Multiplexed HTTP/2 clients and bounded Triple frame transport.
//!
//! Payloads are opaque bytes. RPC dispatch, message cardinality, final status,
//! deadlines and TLS identity configuration belong to the calling protocol layer.

mod client;
mod error;
mod stream;

pub use client::Client;
pub use error::{Error, Result};
pub use stream::{Receiver, Sender};
pub use tokio_util::sync::CancellationToken;

/// Connection header, frame payload and concurrent stream bounds.
///
/// ```
/// let limits = triple_transport::Limits::default();
/// assert_eq!(limits.max_message_size, 4 * 1024 * 1024);
/// assert_eq!(limits.max_header_size, 8 * 1024);
/// assert_eq!(limits.max_concurrent_streams, 128);
/// ```
#[derive(Debug, Copy, Clone)]
pub struct Limits {
    /// Maximum bytes in one framed payload, excluding the five-byte prefix.
    pub max_message_size: usize,
    /// Maximum HTTP/2 header list size including 32 bytes per-field overhead.
    pub max_header_size: usize,
    /// Maximum concurrently active server streams on one connection.
    pub max_concurrent_streams: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message_size: 4 * 1024 * 1024,
            max_header_size: 8 * 1024,
            max_concurrent_streams: 128,
        }
    }
}
