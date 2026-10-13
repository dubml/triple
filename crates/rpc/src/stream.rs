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

use crate::context::Context;
use crate::{
    Code, Deadline, Error, Limits, Metadata, Result, Status, decode_message, encode_message,
};
use bytes::Bytes;
use h2::{RecvStream, SendStream};
use prost::Message;
use std::{future::Future, mem};

/// Cardinality of the request and response sides of one RPC.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RpcKind {
    /// One request and one response.
    Unary,
    /// One request and zero or more responses.
    ServerStreaming,
    /// Zero or more requests and one response.
    ClientStreaming,
    /// Independently streamed requests and responses.
    Bidirectional,
}

impl RpcKind {
    /// Whether the request side requires exactly one message.
    pub fn single_request(self) -> bool {
        matches!(self, Self::Unary | Self::ServerStreaming)
    }
    /// Whether the response side requires exactly one message.
    pub fn single_response(self) -> bool {
        matches!(self, Self::Unary | Self::ClientStreaming)
    }
}

/// Whether a message sender owns the request or response body.
pub enum SendRole {
    /// Request DATA ends by half-closing the body.
    Request,
    /// Response DATA ends with final RPC trailers.
    Response,
}

/// Sends bounded Protobuf messages with the method's message cardinality.
pub struct Outgoing {
    sender: transport::Sender,
    limits: Limits,
    single: bool,
    sent: usize,
    role: SendRole,
    trailers: Metadata,
    context: Context,
}

impl Outgoing {
    /// Wraps an HTTP/2 sender with the cardinality selected by a protocol adapter.
    pub fn new(stream: SendStream<Bytes>, limits: Limits, single: bool, role: SendRole) -> Self {
        Self {
            sender: transport::Sender::new(stream, limits.max_message_size),
            limits,
            single,
            sent: 0,
            role,
            trailers: Metadata::new(),
            context: Context::default(),
        }
    }

    pub(crate) fn with_context(mut self, context: Context) -> Self {
        self.context = context;
        self
    }

    /// Explicitly cancels pending call operations and resets the stream.
    pub fn cancel(&mut self) {
        self.context.cancellation.cancel();
        self.reset();
    }

    /// Sends one message with bounded flow control; dropping a pending send resets the stream.
    pub async fn send<M: Message>(&mut self, message: &M) -> Result<()> {
        if self.single && self.sent != 0 {
            return Err(Error::Cardinality("outgoing"));
        }
        let payload = encode_message(message, self.limits.max_message_size)?;
        let result = self
            .context
            .run(async { self.sender.send(&payload).await.map_err(Error::from) })
            .await;
        if result.is_err() {
            self.reset();
        }
        result?;
        self.sent += 1;
        Ok(())
    }

    /// Half-closes the request side; responses may continue to arrive.
    pub fn close(&mut self) -> Result<()> {
        if matches!(self.role, SendRole::Response) {
            return Err(Error::InvalidHeader("response requires final trailers"));
        }
        if self.single && self.sent != 1 {
            return Err(Error::Cardinality("outgoing"));
        }
        Ok(self.sender.close()?)
    }

    /// Mutably borrows metadata for final server response trailers.
    pub fn trailers_mut(&mut self) -> &mut Metadata {
        &mut self.trailers
    }

    /// Sends final response trailers, validating single-response methods.
    pub fn finish(&mut self, status: &Status) -> Result<()> {
        if status.code() == Code::Ok && self.single && self.sent != 1 {
            return Err(Error::Cardinality("response"));
        }
        let trailers =
            headers::status_headers(status, &self.trailers, self.limits.max_header_size)?;
        Ok(self.sender.finish(trailers)?)
    }

    /// Resets this RPC stream, including its independently driven receive side.
    pub fn reset(&mut self) {
        self.sender.reset();
    }

    /// Resolves when the peer resets the stream or its connection closes.
    pub fn reset_signal(&self) -> impl Future<Output = Result<()>> + use<> {
        let reset = self.sender.reset_signal();
        async move { Ok(reset.await?) }
    }
}

enum ReceiveState {
    Reading,
    TrailersOnly(Status),
    Closed,
}

/// Receives bounded Protobuf messages, enforcing cardinality and final status.
pub struct Incoming {
    receiver: transport::Receiver,
    state: ReceiveState,
    limits: Limits,
    expect_status: bool,
    single: bool,
    received: usize,
    metadata: Metadata,
    trailers: Metadata,
    context: Context,
}

impl Incoming {
    /// Wraps an HTTP/2 body with method cardinality and response-status policy.
    pub fn new(
        stream: RecvStream,
        metadata: Metadata,
        limits: Limits,
        single: bool,
        expect_status: bool,
    ) -> Self {
        Self {
            receiver: transport::Receiver::new(stream, limits.max_message_size),
            state: ReceiveState::Reading,
            limits,
            expect_status,
            single,
            received: 0,
            metadata,
            trailers: Metadata::new(),
            context: Context::default(),
        }
    }

    pub(crate) fn with_context(mut self, context: Context) -> Self {
        self.context = context;
        self
    }

    /// Returns the absolute deadline for forwarding the remaining downstream budget.
    pub fn deadline(&self) -> Option<Deadline> {
        self.context.deadline
    }

    /// Attaches a trailers-only final status received in the initial headers.
    pub fn trailers_only(mut self, status: Status, trailers: Metadata) -> Self {
        self.state = ReceiveState::TrailersOnly(status);
        self.trailers = trailers;
        self
    }

    /// Whether another DATA or trailer read may be needed, rather than a stored final state.
    pub fn is_reading(&self) -> bool {
        matches!(self.state, ReceiveState::Reading)
    }

    /// Terminates reads after an adapter's deadline or cancellation failure.
    pub fn terminate(&mut self) {
        self.state = ReceiveState::Closed;
    }

    /// Borrows the leading application metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Borrows the trailing metadata, populated after reading to the end.
    pub fn trailers(&self) -> &Metadata {
        &self.trailers
    }

    /// Reads the next message, or the final status at stream end.
    ///
    /// A non-OK final status is returned once as `Error::Rpc`; further reads return `None`.
    pub async fn message<M: Message + Default>(&mut self) -> Result<Option<M>> {
        let result = self.next_payload().await;
        let result =
            result.and_then(|payload| payload.map(|bytes| decode_message(&bytes)).transpose());
        if result.is_err() {
            self.state = ReceiveState::Closed;
        }
        result
    }

    async fn next_payload(&mut self) -> Result<Option<Bytes>> {
        match mem::replace(&mut self.state, ReceiveState::Reading) {
            ReceiveState::Closed => {
                self.state = ReceiveState::Closed;
                return Ok(None);
            }
            ReceiveState::TrailersOnly(status) => {
                self.state = ReceiveState::Closed;
                return self.final_status(status);
            }
            ReceiveState::Reading => {}
        }
        let payload = self
            .context
            .run(async { self.receiver.message().await.map_err(Error::from) })
            .await?;
        if let Some(payload) = payload {
            if self.single && self.received != 0 {
                return Err(Error::Cardinality("incoming"));
            }
            self.received += 1;
            return Ok(Some(payload));
        }
        self.state = ReceiveState::Closed;
        if let Some(trailers) = self.receiver.trailers() {
            headers::check_headers(trailers, self.limits.max_header_size)?;
            self.trailers =
                Metadata::from_headers_with_reserved(trailers, headers::is_grpc_reserved_header)?;
            if self.expect_status {
                return self.final_status(headers::parse_status(
                    trailers,
                    self.limits.max_header_size,
                )?);
            }
        }
        if self.expect_status {
            return Err(headers::Error::MissingStatus.into());
        }
        self.final_status(Status::ok())
    }

    fn final_status(&self, status: Status) -> Result<Option<Bytes>> {
        if status.code() != Code::Ok {
            return Err(Error::Rpc(status));
        }
        if self.single && self.received != 1 {
            return Err(Error::Cardinality("incoming"));
        }
        Ok(None)
    }
}

impl crate::Reader for Incoming {
    type Error = Error;
    async fn message<M: Message + Default>(&mut self) -> Result<Option<M>> {
        Incoming::message(self).await
    }
}

impl crate::Writer for Outgoing {
    type Error = Error;
    async fn send<M: Message>(&mut self, message: &M) -> Result<()> {
        Outgoing::send(self, message).await
    }
    fn close(&mut self) -> Result<()> {
        Outgoing::close(self)
    }
    fn finish(&mut self, status: &Status) -> Result<()> {
        Outgoing::finish(self, status)
    }
}
