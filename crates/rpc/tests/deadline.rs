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

//! Timeout grammar, expiration and forwarding tests.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;
use triple_rpc::{Deadline, Error};

#[test]
fn all_timeout_units_and_eight_digit_boundary() {
    for value in ["1H", "1M", "1S", "1m", "100000u", "99999999n"] {
        let deadline = Deadline::from_header(value).unwrap();
        let remaining = deadline.to_header().unwrap();
        assert!(remaining.len() <= 9);
        Deadline::from_header(&remaining).unwrap();
    }
}

#[test]
fn malformed_timeouts_are_rejected() {
    for value in [
        "",
        "S",
        "-1S",
        "+1S",
        "1s",
        "1X",
        "100000000S",
        "1.2S",
        " 1S",
        "1S ",
        "éS",
    ] {
        assert!(
            matches!(Deadline::from_header(value), Err(Error::InvalidHeader(_))),
            "{value}"
        );
    }
}

#[tokio::test]
async fn forwarding_does_not_reset_deadline_and_zero_expires() {
    let deadline = Deadline::after(Duration::from_millis(40)).unwrap();
    let first = deadline.to_header().unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_ne!(first, deadline.to_header().unwrap());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(matches!(deadline.to_header(), Err(Error::DeadlineExceeded)));
    assert!(matches!(
        Deadline::from_header("0n").unwrap().to_header(),
        Err(Error::DeadlineExceeded)
    ));
}
