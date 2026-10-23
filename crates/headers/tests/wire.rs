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

//! Independent vectors for the public header and final-status API.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use triple_headers::{
    Code, Error, Metadata, Status, check_request_headers, check_response_headers, http_error,
    parse_status, request_headers, response_headers, status_headers, validate_request,
};

#[test]
fn request_fields_preserve_authority_path_metadata_and_raw_timeout() {
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace-id", "first").unwrap();
    metadata.append_ascii("trace-id", "second").unwrap();
    metadata.append_binary("trace-bin", &[0, 255]).unwrap();
    let fields = request_headers(&metadata, Some("125m"), 8192).unwrap();
    assert_eq!(fields["content-type"], "application/grpc+proto");
    assert_eq!(fields["te"], "trailers");
    assert_eq!(fields["grpc-accept-encoding"], "identity");
    assert_eq!(fields["trace-bin"], "AP8");
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("https://control.example:8443/Test.Service/Call")
        .body(())
        .unwrap();
    *request.headers_mut() = fields;
    assert_eq!(
        request.uri().authority().unwrap().as_str(),
        "control.example:8443"
    );
    assert_eq!(request.uri().path(), "/Test.Service/Call");
    check_request_headers(&request, 8192).unwrap();
    assert_eq!(
        validate_request(
            request.method(),
            request.uri().path(),
            request.headers(),
            8192
        )
        .unwrap(),
        Some("125m")
    );
    let parsed = Metadata::from_headers(request.headers()).unwrap();
    assert_eq!(
        parsed.ascii_values("trace-id").collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(parsed.binary_values("trace-bin").unwrap(), [vec![0, 255]]);
    assert!(parsed.ascii_values("grpc-timeout").next().is_none());
    assert!(parsed.ascii_values("content-type").next().is_none());
}

#[test]
fn normal_trailers_and_trailers_only_have_the_same_final_status() {
    let mut metadata = Metadata::new();
    metadata.append_ascii("result", "first").unwrap();
    metadata.append_ascii("result", "second").unwrap();
    let status = Status::new(Code::PermissionDenied, "denied: 测试 %")
        .with_details(&[8, 7])
        .unwrap();
    let trailers = status_headers(&status, &metadata, 8192).unwrap();
    assert_eq!(trailers["grpc-status"], "7");
    assert_eq!(trailers["grpc-message"], "denied: %E6%B5%8B%E8%AF%95 %25");
    assert_eq!(trailers["grpc-status-details-bin"], "CAc");
    assert_eq!(parse_status(&trailers, 8192).unwrap(), status);
    let mut immediate = response_headers(&Metadata::new(), 8192).unwrap();
    immediate.extend(trailers);
    check_response_headers(&immediate, 8192).unwrap();
    assert_eq!(parse_status(&immediate, 8192).unwrap(), status);
    let received = Metadata::from_headers(&immediate).unwrap();
    assert_eq!(
        received.ascii_values("result").collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(received.ascii_values("grpc-status").next().is_none());
    assert!(matches!(
        parse_status(&response_headers(&Metadata::new(), 8192).unwrap(), 8192),
        Err(Error::MissingStatus)
    ));
}

#[test]
fn required_request_fields_and_duplicate_protocol_fields_are_rejected() {
    for field in ["content-type", "te"] {
        let mut fields = request_headers(&Metadata::new(), None, 8192).unwrap();
        fields.remove(field);
        assert!(validate_request(&Method::POST, "/s/m", &fields, 8192).is_err());
    }
    for (field, value) in [
        ("content-type", "application/grpc+proto"),
        ("te", "trailers"),
        ("grpc-timeout", "1S"),
        ("grpc-encoding", "identity"),
    ] {
        let mut fields = request_headers(&Metadata::new(), Some("1S"), 8192).unwrap();
        if field == "grpc-encoding" {
            fields.insert(field, HeaderValue::from_static(value));
        }
        fields.append(field, HeaderValue::from_static(value));
        assert!(matches!(
            validate_request(&Method::POST, "/s/m", &fields, 8192),
            Err(Error::InvalidHeader("duplicate protocol header"))
        ));
    }
    let mut fields = request_headers(&Metadata::new(), None, 8192).unwrap();
    fields.insert("te", HeaderValue::from_static("other"));
    assert!(matches!(
        validate_request(&Method::POST, "/s/m", &fields, 8192),
        Err(Error::InvalidHeader("te"))
    ));
    fields.insert("te", HeaderValue::from_static("trailers"));
    assert!(
        validate_request(&Method::POST, "/s/m", &fields, 8192)
            .unwrap()
            .is_none()
    );
}

#[test]
fn invalid_final_status_fields_and_rich_status_cannot_report_success() {
    for value in ["17", "abc", ""] {
        let mut fields = HeaderMap::new();
        fields.insert("grpc-status", HeaderValue::from_str(value).unwrap());
        assert_eq!(parse_status(&fields, 8192).unwrap().code(), Code::Unknown);
    }
    for value in ["!", "CAg=", "/w=="] {
        let mut fields = HeaderMap::new();
        fields.insert("grpc-status", HeaderValue::from_static("7"));
        fields.insert("grpc-status-details-bin", HeaderValue::from_static(value));
        assert!(parse_status(&fields, 8192).is_err());
    }
    let mut fields = HeaderMap::new();
    fields.insert("grpc-status", HeaderValue::from_static("0"));
    fields.insert("grpc-status-details-bin", HeaderValue::from_static("CAA="));
    assert!(parse_status(&fields, 8192).is_err());
    fields.remove("grpc-status-details-bin");
    fields.append("grpc-message", HeaderValue::from_static("first"));
    fields.append("grpc-message", HeaderValue::from_static("second"));
    assert!(parse_status(&fields, 8192).is_err());
}

#[test]
fn non_success_http_responses_map_to_fixed_rpc_codes() {
    for (http, rpc) in [
        (400, Code::InvalidArgument),
        (401, Code::Unauthenticated),
        (403, Code::PermissionDenied),
        (404, Code::Unimplemented),
        (408, Code::DeadlineExceeded),
        (409, Code::Aborted),
        (412, Code::FailedPrecondition),
        (413, Code::ResourceExhausted),
        (415, Code::Internal),
        (429, Code::Unavailable),
        (431, Code::ResourceExhausted),
        (502, Code::Unavailable),
        (503, Code::Unavailable),
        (504, Code::Unavailable),
        (500, Code::Unknown),
        (418, Code::Unknown),
    ] {
        assert_eq!(http_error(StatusCode::from_u16(http).unwrap()).code(), rpc);
    }
}
