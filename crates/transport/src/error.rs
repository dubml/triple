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

/// Errors from framing, connection driving or a closed HTTP/2 stream.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A connection limit or shutdown duration cannot be represented.
    #[error("invalid transport configuration: {0}")]
    InvalidConfig(&'static str),
    /// A bounded frame could not be encoded or decoded.
    #[error(transparent)]
    Frame(#[from] codec::Error),
    /// The HTTP/2 connection or stream failed.
    #[error("HTTP/2 error: {0}")]
    Transport(#[from] h2::Error),
    /// Opening the socket failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// An accepted stream handler panicked or was unexpectedly cancelled.
    #[error("stream handler task failed: {0}")]
    HandlerTask(#[from] tokio::task::JoinError),
    /// The sending half is no longer usable.
    #[error("HTTP/2 stream is closed")]
    StreamClosed,
}

/// A result with this transport's typed error.
pub type Result<T> = std::result::Result<T, Error>;
