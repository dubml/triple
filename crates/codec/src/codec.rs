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

use bytes::{BufMut, Bytes, BytesMut};

use crate::{Error, Result};

const PREFIX_SIZE: usize = 5;

#[derive(Debug)]
enum State {
    Header {
        bytes: [u8; PREFIX_SIZE],
        used: usize,
    },
    Payload {
        length: usize,
        bytes: BytesMut,
    },
    Closed,
}

impl State {
    fn header() -> Self {
        Self::Header {
            bytes: [0; PREFIX_SIZE],
            used: 0,
        }
    }
}

/// Incrementally decodes at most one bounded message at a time.
///
/// Input bytes beyond the returned message remain in the caller's `Bytes`.
/// A malformed frame permanently closes the decoder.
///
/// ```
/// use bytes::Bytes;
/// use triple_codec::FrameDecoder;
/// let mut decoder = FrameDecoder::new(16);
/// let mut wire = Bytes::from_static(&[0, 0, 0, 0, 2, 8, 7]);
/// assert_eq!(decoder.decode(&mut wire)?, Some(Bytes::from_static(&[8, 7])));
/// assert!(wire.is_empty());
/// decoder.finish()?;
/// # Ok::<(), triple_codec::Error>(())
/// ```
#[derive(Debug)]
pub struct FrameDecoder {
    state: State,
    max_message_size: usize,
}

impl FrameDecoder {
    /// Sets the per-message payload limit without allocating a payload buffer.
    pub fn new(max_message_size: usize) -> Self {
        Self {
            state: State::header(),
            max_message_size,
        }
    }

    /// Consumes input until one message is ready or more bytes are required.
    pub fn decode(&mut self, input: &mut Bytes) -> Result<Option<Bytes>> {
        loop {
            let state = std::mem::replace(&mut self.state, State::Closed);
            match state {
                State::Header {
                    mut bytes,
                    mut used,
                } => {
                    let count = input.len().min(PREFIX_SIZE - used);
                    bytes[used..used + count].copy_from_slice(&input.split_to(count));
                    used += count;
                    if used < PREFIX_SIZE {
                        self.state = State::Header { bytes, used };
                        return Ok(None);
                    }
                    match bytes[0] {
                        0 => {}
                        1 => return Err(Error::UnsupportedCompression),
                        flag => return Err(Error::InvalidCompressionFlag(flag)),
                    }
                    let [_, a, b, c, d] = bytes;
                    let length = u32::from_be_bytes([a, b, c, d]) as usize;
                    check_size(length, self.max_message_size)?;
                    self.state = State::Payload {
                        length,
                        bytes: BytesMut::new(),
                    };
                }
                State::Payload { length, mut bytes } => {
                    let count = input.len().min(length - bytes.len());
                    if bytes.is_empty() && count == length {
                        self.state = State::header();
                        return Ok(Some(input.split_to(count)));
                    }
                    bytes.extend_from_slice(&input.split_to(count));
                    if bytes.len() == length {
                        self.state = State::header();
                        return Ok(Some(bytes.freeze()));
                    }
                    self.state = State::Payload { length, bytes };
                    return Ok(None);
                }
                State::Closed => return Err(Error::DecoderClosed),
            }
        }
    }

    /// Checks for a complete message boundary and permanently closes the decoder.
    pub fn finish(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Closed) {
            State::Header { used: 0, .. } => Ok(()),
            State::Closed => Err(Error::DecoderClosed),
            _ => Err(Error::TruncatedFrame),
        }
    }
}

/// Encodes an uncompressed payload after checking both configured and wire limits.
pub fn encode_frame(payload: &[u8], max_message_size: usize) -> Result<Bytes> {
    check_size(payload.len(), max_message_size)?;
    let length = u32::try_from(payload.len()).map_err(|_| Error::LimitExceeded {
        kind: "message",
        size: payload.len(),
        limit: u32::MAX as usize,
    })?;
    let capacity = payload
        .len()
        .checked_add(PREFIX_SIZE)
        .ok_or(Error::LimitExceeded {
            kind: "message",
            size: payload.len(),
            limit: usize::MAX - PREFIX_SIZE,
        })?;
    let mut output = BytesMut::with_capacity(capacity);
    output.put_u8(0);
    output.put_u32(length);
    output.extend_from_slice(payload);
    Ok(output.freeze())
}

fn check_size(size: usize, limit: usize) -> Result<()> {
    if size > limit {
        return Err(Error::LimitExceeded {
            kind: "message",
            size,
            limit,
        });
    }
    Ok(())
}
