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

/// Errors from Triple request fields, metadata or final status.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A header list exceeds its configured byte bound.
    #[error("{kind} size {size} exceeds limit {limit}")]
    LimitExceeded {
        /// What exceeded the bound.
        kind: &'static str,
        /// Observed header list size.
        size: usize,
        /// Configured maximum.
        limit: usize,
    },
    /// Only uncompressed messages are supported.
    #[error("compressed messages are unsupported")]
    UnsupportedCompression,
    /// A request, response or metadata field violates the wire contract.
    #[error("invalid protocol field: {0}")]
    InvalidHeader(&'static str),
    /// The peer ended a response without its required final status.
    #[error("response ended without grpc-status")]
    MissingStatus,
    /// The serialized rich status payload is invalid Protobuf.
    #[error("invalid Protobuf message: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// A result with this library's typed error.
pub type Result<T> = std::result::Result<T, Error>;
