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

use prost::Message;
use std::future::Future;

use crate::{Code, Incoming, Metadata, Outgoing, RpcKind, Status};

/// Error policy supplied by an RPC adapter, preserving its native error type.
pub trait CallError: Sized {
    /// Creates an error for an invalid single-message side.
    fn cardinality(side: &'static str) -> Self;
    /// Identifies a cardinality failure requiring a final Internal status.
    fn is_cardinality(&self) -> bool;
    /// Consumes the error into its final wire status.
    fn into_status(self) -> Status;
}

/// Typed message reader used by the shared RPC call and dispatcher.
pub trait Reader {
    /// The calling protocol layer's error type.
    type Error: CallError;
    /// Reads one decoded message, checking final status at EOF.
    fn message<M: Message + Default>(
        &mut self,
    ) -> impl Future<Output = std::result::Result<Option<M>, Self::Error>> + Send;
}

/// Typed message writer used by the shared RPC call and dispatcher.
pub trait Writer {
    /// The calling protocol layer's error type.
    type Error: CallError;
    /// Sends one encoded message.
    fn send<M: Message>(
        &mut self,
        message: &M,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send;
    /// Half-closes the request after checking its message count.
    fn close(&mut self) -> std::result::Result<(), Self::Error>;
    /// Sends final response status and checks successful response cardinality.
    fn finish(&mut self, status: &Status) -> std::result::Result<(), Self::Error>;
}

/// The stage at which a service handler panicked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandlerPanicPhase {
    /// Calling the service to construct its handler future.
    Construction,
    /// Polling the handler future.
    Future,
}

/// Service lookup and typed handler interface for all RPC modes.
///
/// Adapters may supply their own reader/writer types to preserve deadline and
/// cancellation policy without making this crate depend on that policy.
pub trait Service<R = Incoming, W = Outgoing>: Send + Sync
where
    R: Reader,
    W: Writer<Error = R::Error>,
{
    /// Returns whether the case-sensitive method path is registered.
    fn contains(&self, path: &str) -> bool;
    /// Returns the method cardinality; existing Unary dispatchers keep their default.
    fn kind(&self, path: &str) -> Option<RpcKind> {
        self.contains(path).then_some(RpcKind::Unary)
    }
    /// Supplies leading response metadata before the first DATA frame.
    fn response_metadata(
        &self,
        _path: &str,
        _request: &Metadata,
    ) -> std::result::Result<Metadata, R::Error> {
        Ok(Metadata::new())
    }
    /// Observes a handler panic isolated by the hyper server, without its payload.
    fn on_handler_panic(&self, _path: &str, _phase: HandlerPanicPhase) {}
    /// Handles request and response messages; errors become final trailers.
    fn call(
        &self,
        path: &str,
        request: &mut R,
        response: &mut W,
    ) -> impl Future<Output = std::result::Result<(), R::Error>> + Send;
}

/// Resolves a method lookup or returns the standard unknown-method status.
pub fn resolve_method<T>(method: Option<T>) -> std::result::Result<T, Status> {
    method.ok_or_else(|| Status::new(Code::Unimplemented, "unknown method"))
}

/// Runs the complete Unary request/response exchange and verifies the final status.
pub async fn unary<Q, P, R, W, F>(
    request: &Q,
    requests: &mut W,
    response: F,
) -> std::result::Result<P, W::Error>
where
    Q: Message,
    P: Message + Default,
    R: Reader<Error = W::Error>,
    W: Writer,
    F: Future<Output = std::result::Result<R, W::Error>>,
{
    requests.send(request).await?;
    requests.close()?;
    let mut incoming = response.await?;
    let response = incoming
        .message()
        .await?
        .ok_or_else(|| W::Error::cardinality("response"))?;
    if incoming.message::<P>().await?.is_some() {
        return Err(W::Error::cardinality("response"));
    }
    Ok(response)
}

// Unknown fields are accepted while malformed Protobuf is still rejected.
#[derive(prost::Message)]
struct OpaqueMessage {}

/// Calls a handler and drains single-request methods to verify their cardinality.
pub async fn invoke_service<S, R, W>(
    service: &S,
    path: &str,
    kind: RpcKind,
    request: &mut R,
    response: &mut W,
) -> std::result::Result<(), R::Error>
where
    R: Reader,
    W: Writer<Error = R::Error>,
    S: Service<R, W>,
{
    service.call(path, request, response).await?;
    if kind.single_request() {
        while request.message::<OpaqueMessage>().await?.is_some() {}
    }
    Ok(())
}

/// Sends one final status, including Internal for missing or excess successful responses.
pub fn finish_call<W: Writer>(
    response: &mut W,
    result: std::result::Result<(), W::Error>,
) -> std::result::Result<(), W::Error> {
    let status = result.map_or_else(CallError::into_status, |()| Status::ok());
    if let Err(error) = response.finish(&status) {
        if error.is_cardinality() {
            response.finish(&error.into_status())?;
        } else {
            return Err(error);
        }
    }
    Ok(())
}
