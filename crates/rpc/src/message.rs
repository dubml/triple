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

use crate::{Error, Result};
use prost::Message;

/// Encodes a Protobuf message after checking its payload limit.
///
/// ```
/// #[derive(prost::Message)]
/// struct Echo { #[prost(string, tag = "1")] text: String }
/// let payload = rpc::encode_message(&Echo { text: "A".into() }, 3)?;
/// assert_eq!(payload, [10, 1, b'A']);
/// # Ok::<(), rpc::Error>(())
/// ```
pub fn encode_message<M: Message>(message: &M, limit: usize) -> Result<Vec<u8>> {
    let size = message.encoded_len();
    if size > limit {
        return Err(Error::LimitExceeded {
            kind: "message",
            size,
            limit,
        });
    }
    Ok(message.encode_to_vec())
}

/// Decodes a message from a borrowed Protobuf payload.
pub fn decode_message<M: Message + Default>(payload: &[u8]) -> Result<M> {
    Ok(M::decode(payload)?)
}
