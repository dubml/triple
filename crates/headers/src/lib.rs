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

//! Triple request fields, application metadata and final RPC status.
//!
//! Header list limits are explicit byte bounds including per-field HTTP/2 overhead.
//! Timeout values are forwarded here; deadline parsing and enforcement belong to the caller.

mod error;
mod headers;
mod metadata;
mod status;

pub use error::{Error, Result};
pub use headers::{
    check_headers, check_request_headers, check_response_headers, http_error, parse_status,
    request_headers, response_headers, status_headers, validate_content_type, validate_encoding,
    validate_path, validate_request,
};
pub use metadata::{Metadata, is_grpc_reserved_header};
pub use status::{Code, Status};
pub use triple_core::{parse_grpc_timeout, parse_tri_service_timeout};
