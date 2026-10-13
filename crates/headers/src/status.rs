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

pub use triple_core::Code;

pub(crate) fn parse_code(value: &str) -> Code {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Code::Unknown;
    }
    value
        .parse::<u8>()
        .ok()
        .and_then(|code| Code::try_from(code).ok())
        .unwrap_or(Code::Unknown)
}

/// Final RPC status, including an optional encoded `google.rpc.Status` payload.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub struct Status {
    semantic: triple_core::Status,
    details: Vec<u8>,
}

impl Status {
    /// Creates a final status from a borrowed human-readable message.
    pub fn new(code: Code, message: &str) -> Self {
        Self {
            semantic: triple_core::Status::new(code, message),
            details: Vec::new(),
        }
    }

    /// Creates the successful final status.
    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    /// Attaches a valid serialized `google.rpc.Status` matching this non-OK status.
    pub fn with_details(mut self, details: &[u8]) -> Result<Self> {
        headers::validate_status_details(self.code(), details)?;
        self.details = details.to_vec();
        Ok(self)
    }

    /// Returns the RPC status code.
    pub fn code(&self) -> Code {
        self.semantic.code()
    }

    /// Returns the human-readable status message.
    pub fn message(&self) -> &str {
        self.semantic.message()
    }

    /// Returns the serialized rich-error payload.
    pub fn details(&self) -> &[u8] {
        &self.details
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.semantic.fmt(f)
    }
}

impl AsRef<triple_core::Status> for Status {
    fn as_ref(&self) -> &triple_core::Status {
        &self.semantic
    }
}

impl From<triple_core::Status> for Status {
    fn from(semantic: triple_core::Status) -> Self {
        Self {
            semantic,
            details: Vec::new(),
        }
    }
}
