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

use std::{
    future::{Future, poll_fn},
    mem,
    sync::{Arc, Mutex},
    task::Poll,
};

use bytes::Bytes;
use codec::{FrameDecoder, encode_frame};
use h2::{Reason, RecvStream, SendStream};
use http::HeaderMap;

use crate::{Error, Result};

enum SendState {
    Open(SendStream<Bytes>),
    Closed,
}

struct SendGuard(Option<Arc<Mutex<SendState>>>);

impl Drop for SendGuard {
    fn drop(&mut self) {
        if let Some(state) = &self.0 {
            reset_state(state);
        }
    }
}

fn reset_state(state: &Mutex<SendState>) {
    if let Ok(mut state) = state.lock()
        && let SendState::Open(mut stream) = mem::replace(&mut *state, SendState::Closed)
    {
        stream.send_reset(Reason::CANCEL);
    }
}

/// Sends bounded opaque payloads as Triple frames under HTTP/2 flow control.
///
/// One frame is retained while waiting for capacity. Dropping a pending send
/// resets the stream because a partially sent frame cannot safely be reused.
pub struct Sender {
    state: Arc<Mutex<SendState>>,
    max_message_size: usize,
}

impl Sender {
    /// Wraps one sending half with an explicit per-payload bound.
    pub fn new(stream: SendStream<Bytes>, max_message_size: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(SendState::Open(stream))),
            max_message_size,
        }
    }

    /// Sends one payload without buffering subsequent messages.
    pub async fn send(&mut self, payload: &[u8]) -> Result<()> {
        let mut frame = encode_frame(payload, self.max_message_size)?;
        self.with_stream(|stream| {
            stream.reserve_capacity(frame.len());
            Ok(())
        })?;
        let mut guard = SendGuard(Some(Arc::clone(&self.state)));
        let result: Result<()> = async {
            while !frame.is_empty() {
                let capacity = poll_fn(|cx| {
                    let Ok(mut state) = self.state.lock() else {
                        return Poll::Ready(Err(Error::StreamClosed));
                    };
                    let SendState::Open(stream) = &mut *state else {
                        return Poll::Ready(Err(Error::StreamClosed));
                    };
                    stream.poll_capacity(cx).map(|capacity| {
                        capacity
                            .ok_or(Error::StreamClosed)
                            .and_then(|value| value.map_err(Error::from))
                    })
                })
                .await?;
                if capacity == 0 {
                    continue;
                }
                let count = capacity.min(frame.len());
                self.with_stream(|stream| {
                    stream.send_data(frame.split_to(count), false)?;
                    stream.reserve_capacity(frame.len());
                    Ok(())
                })?;
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.reset();
        }
        result?;
        guard.0 = None;
        Ok(())
    }

    /// Half-closes this sending direction with END_STREAM; receiving may continue.
    pub fn close(&mut self) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::StreamClosed)?;
        let SendState::Open(mut stream) = mem::replace(&mut *state, SendState::Closed) else {
            return Err(Error::StreamClosed);
        };
        stream.send_data(Bytes::new(), true)?;
        Ok(())
    }

    /// Sends final trailers and closes this sending direction.
    pub fn finish(&mut self, trailers: HeaderMap) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::StreamClosed)?;
        let SendState::Open(mut stream) = mem::replace(&mut *state, SendState::Closed) else {
            return Err(Error::StreamClosed);
        };
        stream.send_trailers(trailers)?;
        Ok(())
    }

    /// Resets the current stream with CANCEL while preserving other streams.
    pub fn reset(&mut self) {
        reset_state(&self.state);
    }

    fn with_stream<T>(
        &self,
        action: impl FnOnce(&mut SendStream<Bytes>) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.state.lock().map_err(|_| Error::StreamClosed)?;
        let SendState::Open(stream) = &mut *state else {
            return Err(Error::StreamClosed);
        };
        action(stream)
    }

    /// Creates an owned future watching peer reset without borrowing the sender.
    pub fn reset_signal(&self) -> impl Future<Output = Result<()>> + use<> {
        // The sender and reset watcher hold the mutex only for synchronous
        // polling or mutation, never across an await point.
        let state = Arc::clone(&self.state);
        poll_fn(move |cx| {
            let Ok(mut state) = state.lock() else {
                return Poll::Ready(Err(Error::StreamClosed));
            };
            let SendState::Open(stream) = &mut *state else {
                return Poll::Ready(Err(Error::StreamClosed));
            };
            stream
                .poll_reset(cx)
                .map(|result| result.map(|_| ()).map_err(Error::from))
        })
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.reset();
    }
}

enum ReceiveState {
    Reading,
    Closed,
}

/// Incrementally receives bounded frames without queuing an entire stream.
///
/// At most one partial message and one HTTP/2 DATA chunk are retained. Receive
/// window capacity is released only while the consumer asks for another message.
pub struct Receiver {
    stream: RecvStream,
    input: Bytes,
    decoder: FrameDecoder,
    state: ReceiveState,
    trailers: Option<HeaderMap>,
}

impl Receiver {
    /// Wraps a receiving half with an explicit per-payload bound.
    pub fn new(stream: RecvStream, max_message_size: usize) -> Self {
        Self {
            stream,
            input: Bytes::new(),
            decoder: FrameDecoder::new(max_message_size),
            state: ReceiveState::Reading,
            trailers: None,
        }
    }

    /// Reads one opaque payload or checks the final frame boundary at EOF.
    pub async fn message(&mut self) -> Result<Option<Bytes>> {
        if matches!(self.state, ReceiveState::Closed) {
            return Ok(None);
        }
        let result = self.next_payload().await;
        if result.is_err() {
            self.state = ReceiveState::Closed;
        }
        result
    }

    /// Borrows raw trailers after reading through EOF, without interpreting RPC status.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }

    async fn next_payload(&mut self) -> Result<Option<Bytes>> {
        loop {
            if let Some(payload) = self.decoder.decode(&mut self.input)? {
                return Ok(Some(payload));
            }
            if let Some(data) = self.stream.data().await.transpose()? {
                self.stream.flow_control().release_capacity(data.len())?;
                self.input = data;
                continue;
            }
            self.decoder.finish()?;
            self.state = ReceiveState::Closed;
            self.trailers = self.stream.trailers().await?;
            return Ok(None);
        }
    }
}
