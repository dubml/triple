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

/// Errors from Triple message framing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A frame declares an unknown compression flag.
    #[error("invalid compression flag {0}")]
    InvalidCompressionFlag(u8),
    /// Only uncompressed messages are supported.
    #[error("compressed messages are unsupported")]
    UnsupportedCompression,
    /// A message exceeds its configured bound.
    #[error("{kind} size {size} exceeds limit {limit}")]
    LimitExceeded {
        /// What exceeded the bound.
        kind: &'static str,
        /// Observed or declared size.
        size: usize,
        /// Configured maximum.
        limit: usize,
    },
    /// The stream ended partway through a message.
    #[error("truncated message at end of stream")]
    TruncatedFrame,
    /// The decoder cannot be reused after EOF or a framing error.
    #[error("decoder is already finished or failed")]
    DecoderClosed,
}

/// A result with a framing error.
pub type Result<T> = std::result::Result<T, Error>;
