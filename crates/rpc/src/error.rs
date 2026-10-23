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

use crate::{CallError, Code, Status};

/// Errors from RPC fields, message encoding, transport or a remote status.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A message exceeds its configured payload limit.
    #[error("{kind} size {size} exceeds limit {limit}")]
    LimitExceeded {
        /// What exceeded its bound.
        kind: &'static str,
        /// Encoded payload size.
        size: usize,
        /// Configured maximum.
        limit: usize,
    },
    /// A request or response field is invalid.
    #[error("invalid protocol field: {0}")]
    InvalidHeader(&'static str),
    /// Header, metadata or final-status validation failed.
    #[error(transparent)]
    Headers(#[from] triple_headers::Error),
    /// Framing, stream or connection transport failed.
    #[error(transparent)]
    Transport(#[from] triple_transport::Error),
    /// Protobuf decoding failed.
    #[error("invalid Protobuf message: {0}")]
    Decode(#[from] prost::DecodeError),
    /// Building an HTTP request or response failed.
    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),
    /// Socket or TLS I/O failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// TLS configuration or peer authentication failed.
    #[error("TLS error: {0}")]
    Tls(#[from] tokio_rustls::rustls::Error),
    /// The local or propagated deadline expired.
    #[error("RPC deadline exceeded")]
    DeadlineExceeded,
    /// The caller cancelled the RPC.
    #[error("RPC cancelled")]
    Cancelled,
    /// Method message cardinality is invalid.
    #[error("expected exactly one {0} message")]
    Cardinality(&'static str),
    /// The remote or local service returned an RPC error status.
    #[error("{0}")]
    Rpc(#[from] Status),
}

/// A result with a typed RPC error.
pub type Result<T> = std::result::Result<T, Error>;

impl CallError for Error {
    fn cardinality(side: &'static str) -> Self {
        Self::Cardinality(side)
    }
    fn is_cardinality(&self) -> bool {
        matches!(self, Self::Cardinality(_))
    }
    fn into_status(self) -> Status {
        let code = match self {
            Self::LimitExceeded { .. }
            | Self::Headers(triple_headers::Error::LimitExceeded { .. })
            | Self::Transport(triple_transport::Error::Frame(
                triple_codec::Error::LimitExceeded { .. },
            )) => Code::ResourceExhausted,
            Self::Headers(triple_headers::Error::UnsupportedCompression)
            | Self::Transport(triple_transport::Error::Frame(
                triple_codec::Error::UnsupportedCompression,
            )) => Code::Unimplemented,
            Self::InvalidHeader(_)
            | Self::Headers(triple_headers::Error::InvalidHeader(_))
            | Self::Headers(triple_headers::Error::Decode(_))
            | Self::Decode(_)
            | Self::Transport(triple_transport::Error::InvalidConfig(_)) => Code::InvalidArgument,
            Self::Cardinality("response" | "outgoing") => Code::Internal,
            Self::Cardinality(_) => Code::InvalidArgument,
            Self::Transport(triple_transport::Error::Io(_)) | Self::Io(_) => Code::Unavailable,
            Self::DeadlineExceeded => Code::DeadlineExceeded,
            Self::Cancelled => Code::Cancelled,
            Self::Transport(triple_transport::Error::Transport(ref error)) => {
                match error.reason() {
                    Some(h2::Reason::CANCEL) => Code::Cancelled,
                    Some(h2::Reason::REFUSED_STREAM) | None => Code::Unavailable,
                    Some(h2::Reason::ENHANCE_YOUR_CALM) => Code::ResourceExhausted,
                    Some(h2::Reason::INADEQUATE_SECURITY) => Code::PermissionDenied,
                    _ => Code::Internal,
                }
            }
            Self::Rpc(status) => return status,
            _ => Code::Internal,
        };
        Status::new(code, &self.to_string())
    }
}
