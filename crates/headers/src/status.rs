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

use crate::{Result, headers};
use std::fmt;

/// Standard gRPC-compatible RPC status codes.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Code {
    /// Successful completion.
    Ok = 0,
    /// Caller cancelled the operation.
    Cancelled = 1,
    /// Unknown failure.
    Unknown = 2,
    /// Invalid argument.
    InvalidArgument = 3,
    /// Deadline expired.
    DeadlineExceeded = 4,
    /// Resource not found.
    NotFound = 5,
    /// Resource already exists.
    AlreadyExists = 6,
    /// Access denied.
    PermissionDenied = 7,
    /// Resource limit reached.
    ResourceExhausted = 8,
    /// Required precondition failed.
    FailedPrecondition = 9,
    /// Operation aborted.
    Aborted = 10,
    /// Argument outside range.
    OutOfRange = 11,
    /// Method or feature unavailable.
    Unimplemented = 12,
    /// Internal protocol failure.
    Internal = 13,
    /// Endpoint temporarily unavailable.
    Unavailable = 14,
    /// Irrecoverable data loss.
    DataLoss = 15,
    /// Invalid or missing credentials.
    Unauthenticated = 16,
}

impl Code {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "0" => Self::Ok,
            "1" => Self::Cancelled,
            "2" => Self::Unknown,
            "3" => Self::InvalidArgument,
            "4" => Self::DeadlineExceeded,
            "5" => Self::NotFound,
            "6" => Self::AlreadyExists,
            "7" => Self::PermissionDenied,
            "8" => Self::ResourceExhausted,
            "9" => Self::FailedPrecondition,
            "10" => Self::Aborted,
            "11" => Self::OutOfRange,
            "12" => Self::Unimplemented,
            "13" => Self::Internal,
            "14" => Self::Unavailable,
            "15" => Self::DataLoss,
            "16" => Self::Unauthenticated,
            _ => return None,
        })
    }
}

/// Final RPC status, including an optional encoded `google.rpc.Status` payload.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub struct Status {
    code: Code,
    message: String,
    details: Vec<u8>,
}

impl Status {
    /// Creates a final status from a borrowed human-readable message.
    pub fn new(code: Code, message: &str) -> Self {
        Self {
            code,
            message: message.to_owned(),
            details: Vec::new(),
        }
    }

    /// Creates the successful final status.
    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    /// Attaches a valid serialized `google.rpc.Status` matching this non-OK status.
    pub fn with_details(mut self, details: &[u8]) -> Result<Self> {
        headers::validate_status_details(self.code, details)?;
        self.details = details.to_vec();
        Ok(self)
    }

    /// Returns the RPC status code.
    pub fn code(&self) -> Code {
        self.code
    }

    /// Returns the human-readable status message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the serialized rich-error payload.
    pub fn details(&self) -> &[u8] {
        &self.details
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}
