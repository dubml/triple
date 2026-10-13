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

use bytes::Bytes;
use http::{Response, StatusCode};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    CancellationToken, Deadline, Error, Incoming, Limits, Metadata, Outgoing, Result, Service,
    Status, context::Context, stream::SendRole,
};

/// Serves all RPC modes on one HTTP/2 connection, using the transport's owned lifecycle.
pub async fn serve_connection<T, S>(
    io: T,
    service: Arc<S>,
    limits: Limits,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service + 'static,
{
    Ok(transport::serve_connection(
        io,
        move |request, response| {
            let service = Arc::clone(&service);
            async move {
                let _ = handle_call(request, response, &*service, limits).await;
            }
        },
        limits,
        shutdown,
        shutdown_timeout,
    )
    .await?)
}

async fn handle_call<S: Service>(
    request: http::Request<h2::RecvStream>,
    mut responder: h2::server::SendResponse<Bytes>,
    service: &S,
    limits: Limits,
) -> Result<()> {
    let path = request
        .uri()
        .path_and_query()
        .map_or("", |value| value.as_str());
    let deadline = match headers::validate_request(
        request.method(),
        path,
        request.headers(),
        limits.max_header_size,
    )
    .map_err(Error::from)
    .and_then(|value| value.map(Deadline::from_header).transpose())
    {
        Ok(deadline) => deadline,
        Err(error) => {
            if matches!(
                error,
                Error::Headers(headers::Error::InvalidHeader("content-type"))
            ) {
                responder
                    .send_response(
                        Response::builder()
                            .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                            .body(())?,
                        true,
                    )
                    .map_err(transport::Error::from)?;
                return Ok(());
            }
            return immediate_error(
                &mut responder,
                &crate::CallError::into_status(error),
                limits,
            );
        }
    };
    let kind = match crate::resolve_method(service.kind(request.uri().path())) {
        Ok(kind) => kind,
        Err(status) => return immediate_error(&mut responder, &status, limits),
    };
    let metadata = match Metadata::from_headers(request.headers()) {
        Ok(metadata) => metadata,
        Err(error) => {
            return immediate_error(
                &mut responder,
                &crate::CallError::into_status(Error::from(error)),
                limits,
            );
        }
    };
    let path = request.uri().path().to_owned();
    let context = Context {
        deadline,
        cancellation: CancellationToken::new(),
    };
    let mut incoming = Incoming::new(
        request.into_body(),
        metadata,
        limits,
        kind.single_request(),
        false,
    )
    .with_context(context.clone());
    let response_metadata = match service.response_metadata(&path, incoming.metadata()) {
        Ok(metadata) => metadata,
        Err(error) => {
            return immediate_error(
                &mut responder,
                &crate::CallError::into_status(error),
                limits,
            );
        }
    };
    let mut response = Response::new(());
    *response.headers_mut() =
        headers::response_headers(&response_metadata, limits.max_header_size)?;
    let stream = responder
        .send_response(response, false)
        .map_err(transport::Error::from)?;
    let mut outgoing = Outgoing::new(stream, limits, kind.single_response(), SendRole::Response)
        .with_context(context.clone());
    let reset = outgoing.reset_signal();
    let result = tokio::select! {
        result = context.run(crate::invoke_service(service, &path, kind, &mut incoming, &mut outgoing)) => result,
        _ = reset => { context.cancellation.cancel(); outgoing.reset(); return Ok(()); }
    };
    crate::finish_call(&mut outgoing, result)
}

/// Sends an immediate trailers-only RPC error response with the configured header bound.
pub fn immediate_error(
    responder: &mut h2::server::SendResponse<Bytes>,
    status: &Status,
    limits: Limits,
) -> Result<()> {
    let mut fields = headers::status_headers(status, &Metadata::new(), limits.max_header_size)?;
    fields.extend(headers::response_headers(
        &Metadata::new(),
        limits.max_header_size,
    )?);
    headers::check_response_headers(&fields, limits.max_header_size)?;
    let mut response = Response::new(());
    *response.headers_mut() = fields;
    responder
        .send_response(response, true)
        .map_err(transport::Error::from)?;
    Ok(())
}
