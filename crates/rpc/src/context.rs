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

use crate::{CancellationToken, Deadline, Error, Result};
use std::future::{Future, pending};

#[derive(Clone, Default)]
pub(crate) struct Context {
    pub(crate) deadline: Option<Deadline>,
    pub(crate) cancellation: CancellationToken,
}

impl Context {
    pub(crate) async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(Error::Cancelled),
            _ = async {
                match self.deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.instant()).await,
                    None => pending().await,
                }
            } => Err(Error::DeadlineExceeded),
            result = operation => result,
        }
    }
}
