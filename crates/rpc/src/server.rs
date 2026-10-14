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

pub use crate::hyper_server::{ResponseBody, handle, serve_connection};
use crate::{Limits, Metadata, Result, Status};
use bytes::Bytes;
use http::Response;

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
