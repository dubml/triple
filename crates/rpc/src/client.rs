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

use http::{Request, Response, StatusCode, Uri};
use prost::Message;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    CancellationToken, Deadline, Error, Incoming, Limits, Metadata, Outgoing, Result, RpcKind,
    Status, context::Context, stream::SendRole,
};

/// Per-call metadata, absolute deadline and explicit cancellation signal.
#[derive(Default)]
pub struct CallOptions {
    /// Leading metadata sent to the server.
    pub metadata: Metadata,
    /// The remaining deadline is propagated to the server.
    pub deadline: Option<Deadline>,
    /// Cancelling this token cancels operations awaiting this RPC.
    pub cancellation: CancellationToken,
}

/// One native Protobuf client on a multiplexed HTTP/2 connection.
pub struct Client {
    transport: transport::Client,
    origin: Uri,
    limits: Limits,
}

impl Client {
    /// Takes the single HTTP/2 PING handle; callers choose interval and timeout.
    pub fn take_ping_pong(&mut self) -> Option<h2::PingPong> {
        self.transport.take_ping_pong()
    }

    /// Connects using plaintext HTTP/2 prior knowledge.
    pub async fn connect(address: &str, limits: Limits) -> Result<Self> {
        let origin = parse_origin(&format!("http://{address}"))?;
        Ok(Self {
            transport: transport::Client::connect(address, limits).await?,
            origin,
            limits,
        })
    }

    /// Establishes HTTP/2 over a caller-provided stream without configuring TLS.
    pub async fn from_io<T>(io: T, origin: &str, limits: Limits) -> Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let origin = parse_origin(origin)?;
        Ok(Self {
            transport: transport::Client::from_io(io, limits).await?,
            origin,
            limits,
        })
    }

    /// Opens a Unary call with independently driven request and response halves.
    pub async fn call(&mut self, path: &str, metadata: &Metadata) -> Result<Call> {
        self.call_stream(path, RpcKind::Unary, metadata).await
    }

    /// Opens any RPC mode with independently driven request and response halves.
    pub async fn call_stream(
        &mut self,
        path: &str,
        kind: RpcKind,
        metadata: &Metadata,
    ) -> Result<Call> {
        self.call_with_options(
            path,
            kind,
            &CallOptions {
                metadata: metadata.clone(),
                ..Default::default()
            },
        )
        .await
    }

    /// Opens an RPC with leading metadata, a propagated deadline and local cancellation.
    pub async fn call_with_options(
        &mut self,
        path: &str,
        kind: RpcKind,
        options: &CallOptions,
    ) -> Result<Call> {
        headers::validate_path(path)?;
        let timeout = options.deadline.map(Deadline::to_header).transpose()?;
        let request = prepare_request(
            &self.origin,
            path,
            &options.metadata,
            timeout.as_deref(),
            self.limits,
        )?;
        let context = Context {
            deadline: options.deadline,
            cancellation: options.cancellation.clone(),
        };
        context
            .run(async { self.transport.ready().await.map_err(Error::from) })
            .await?;
        let (response, stream) = self.transport.send_request(request, false)?;
        Ok(Call {
            requests: Outgoing::new(
                stream,
                self.limits,
                kind.single_request(),
                SendRole::Request,
            )
            .with_context(context.clone()),
            response: PendingResponse {
                response,
                limits: self.limits,
                single: kind.single_response(),
                context,
            },
        })
    }

    /// Invokes a Unary method and verifies both message cardinality and final status.
    pub async fn unary<Q: Message, P: Message + Default>(
        &mut self,
        path: &str,
        request: &Q,
        metadata: &Metadata,
    ) -> Result<P> {
        let Call {
            mut requests,
            response,
        } = self.call(path, metadata).await?;
        crate::unary(request, &mut requests, response.receive()).await
    }
    /// Invokes one Unary method with deadline and explicit cancellation policy.
    pub async fn unary_with_options<Q: Message, P: Message + Default>(
        &mut self,
        path: &str,
        request: &Q,
        options: &CallOptions,
    ) -> Result<P> {
        let Call {
            mut requests,
            response,
        } = self
            .call_with_options(path, RpcKind::Unary, options)
            .await?;
        crate::unary(request, &mut requests, response.receive()).await
    }
}

/// Validates an HTTP origin without an application path or query.
pub fn parse_origin(origin: &str) -> Result<Uri> {
    let origin: Uri = origin
        .parse()
        .map_err(|_| Error::InvalidHeader("origin URI"))?;
    if !matches!(origin.scheme_str(), Some("http" | "https"))
        || origin.authority().is_none()
        || origin.path_and_query().is_some_and(|p| p.as_str() != "/")
    {
        return Err(Error::InvalidHeader("origin URI"));
    }
    Ok(origin)
}

/// Builds a bounded request, forwarding an optional caller-validated raw timeout.
pub fn prepare_request(
    origin: &Uri,
    path: &str,
    metadata: &Metadata,
    timeout: Option<&str>,
    limits: Limits,
) -> Result<Request<()>> {
    headers::validate_path(path)?;
    let mut parts = origin.clone().into_parts();
    parts.path_and_query = Some(path.parse().map_err(|_| Error::InvalidHeader("RPC path"))?);
    let uri = Uri::from_parts(parts).map_err(|_| Error::InvalidHeader("RPC URI"))?;
    let mut request = Request::builder().method("POST").uri(uri).body(())?;
    *request.headers_mut() = headers::request_headers(metadata, timeout, limits.max_header_size)?;
    headers::check_request_headers(&request, limits.max_header_size)?;
    Ok(request)
}

/// Independently driven halves of one RPC, constrained by its method cardinality.
pub struct Call {
    /// Sends request messages then half-closes the request body.
    pub requests: Outgoing,
    /// Awaits and validates response headers.
    pub response: PendingResponse,
}

/// Awaits response headers without owning the request sender.
pub struct PendingResponse {
    response: h2::client::ResponseFuture,
    limits: Limits,
    single: bool,
    context: Context,
}

/// Validated leading response fields with optional trailers-only final status.
pub struct ResponseHead {
    /// The received HTTP/2 body.
    pub body: h2::RecvStream,
    /// Received application metadata.
    pub metadata: Metadata,
    /// Final status present only for a trailers-only response.
    pub status: Option<Status>,
}

/// Validates leading response fields before any Protobuf body is read.
pub fn parse_response(response: Response<h2::RecvStream>, limit: usize) -> Result<ResponseHead> {
    headers::check_response_headers(response.headers(), limit)?;
    if response.status() != StatusCode::OK {
        return Err(headers::http_error(response.status()).into());
    }
    headers::validate_content_type(response.headers())?;
    headers::validate_encoding(response.headers())?;
    let metadata =
        Metadata::from_headers_with_reserved(response.headers(), headers::is_grpc_reserved_header)?;
    let status = response
        .headers()
        .contains_key("grpc-status")
        .then(|| headers::parse_status(response.headers(), limit))
        .transpose()?;
    if status.is_some() && !response.body().is_end_stream() {
        return Err(Error::InvalidHeader("grpc-status before stream end"));
    }
    Ok(ResponseHead {
        body: response.into_body(),
        metadata,
        status,
    })
}

impl PendingResponse {
    /// Receives the message reader after checking media type and initial status.
    pub async fn receive(self) -> Result<Incoming> {
        let head = parse_response(
            self.context
                .run(async {
                    self.response
                        .await
                        .map_err(transport::Error::from)
                        .map_err(Error::from)
                })
                .await?,
            self.limits.max_header_size,
        )?;
        if let Some(status) = head.status {
            let incoming =
                Incoming::new(head.body, Metadata::new(), self.limits, self.single, true)
                    .with_context(self.context);
            return Ok(incoming.trailers_only(status, head.metadata));
        }
        Ok(
            Incoming::new(head.body, head.metadata, self.limits, self.single, true)
                .with_context(self.context),
        )
    }
}
