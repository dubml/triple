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

use std::time::Duration;
use tokio::time::Instant;

use crate::{Error, Result};

/// An absolute deadline; forwarding it does not reset the timeout budget.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline(triple_core::Deadline);

impl Deadline {
    /// Creates a deadline relative to the current monotonic time.
    pub fn after(duration: Duration) -> Result<Self> {
        triple_core::Deadline::after_at(duration, Instant::now().into_std())
            .map(Self)
            .map_err(|_| Error::InvalidHeader("grpc-timeout overflow"))
    }

    /// Parses the six gRPC units and at most eight decimal digits.
    pub fn from_header(value: &str) -> Result<Self> {
        let duration = triple_core::parse_grpc_timeout(value).map_err(|error| match error {
            triple_core::Error::InvalidTimeoutUnit => Error::InvalidHeader("grpc-timeout unit"),
            triple_core::Error::TimeoutOverflow => Error::InvalidHeader("grpc-timeout overflow"),
            _ => Error::InvalidHeader("grpc-timeout"),
        })?;
        Self::after(duration)
    }

    /// Encodes the remaining budget, rounding up by at most one unit.
    pub fn to_header(self) -> Result<String> {
        let nanos = self
            .0
            .instant()
            .checked_duration_since(Instant::now().into_std())
            .ok_or(Error::DeadlineExceeded)?
            .as_nanos();
        for (unit, divisor) in [
            ('n', 1),
            ('u', 1000),
            ('m', 1_000_000),
            ('S', 1_000_000_000),
            ('M', 60_000_000_000),
            ('H', 3_600_000_000_000),
        ] {
            let count = nanos.div_ceil(divisor);
            if count <= 99_999_999 {
                return Ok(format!("{count}{unit}"));
            }
        }
        Err(Error::InvalidHeader("grpc-timeout overflow"))
    }

    pub(crate) fn instant(self) -> Instant {
        Instant::from_std(self.0.instant())
    }
}

impl From<triple_core::Deadline> for Deadline {
    fn from(deadline: triple_core::Deadline) -> Self {
        Self(deadline)
    }
}

impl From<Deadline> for triple_core::Deadline {
    fn from(deadline: Deadline) -> Self {
        deadline.0
    }
}
