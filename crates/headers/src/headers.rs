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

use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use http::{HeaderMap, HeaderValue, Method, StatusCode};

use crate::metadata::decode_binary;
use crate::{Code, Error, Metadata, Result, Status};

/// Checks a header list, including 32 bytes overhead for each field.
pub fn check_headers(headers: &HeaderMap, limit: usize) -> Result<()> {
    check_headers_with_overhead(headers, 0, limit)
}

fn check_headers_with_overhead(headers: &HeaderMap, overhead: usize, limit: usize) -> Result<()> {
    let size = headers.iter().try_fold(overhead, |total, (name, value)| {
        total
            .checked_add(name.as_str().len())
            .and_then(|n| n.checked_add(value.len()))
            .and_then(|n| n.checked_add(32))
            .ok_or(Error::LimitExceeded {
                kind: "headers",
                size: usize::MAX,
                limit,
            })
    })?;
    if size > limit {
        return Err(Error::LimitExceeded {
            kind: "headers",
            size,
            limit,
        });
    }
    Ok(())
}

/// Checks request headers and HTTP/2 pseudo fields against the limit.
pub fn check_request_headers(request: &http::Request<()>, limit: usize) -> Result<()> {
    let uri = request.uri();
    let pseudo_fields = [
        (":method", request.method().as_str()),
        (":scheme", uri.scheme_str().unwrap_or("")),
        (
            ":authority",
            uri.authority().map_or("", |value| value.as_str()),
        ),
        (
            ":path",
            uri.path_and_query().map_or("", |value| value.as_str()),
        ),
    ];
    let overhead = pseudo_fields.iter().fold(0_usize, |total, (name, value)| {
        total
            .saturating_add(name.len())
            .saturating_add(value.len())
            .saturating_add(32)
    });
    check_headers_with_overhead(request.headers(), overhead, limit)
}

/// Checks response headers, including the HTTP/2 status pseudo field.
pub fn check_response_headers(headers: &HeaderMap, limit: usize) -> Result<()> {
    // :status is always a three-digit value in an HTTP response.
    check_headers_with_overhead(headers, 7 + 3 + 32, limit)
}

/// Validates a case-sensitive `/Service/Method` path without a query or fragment.
pub fn validate_path(path: &str) -> Result<()> {
    let mut parts = path.split('/');
    if parts.next() != Some("")
        || parts.next().is_none_or(str::is_empty)
        || parts.next().is_none_or(str::is_empty)
        || parts.next().is_some()
        || path.contains(['?', '#'])
    {
        return Err(Error::InvalidHeader("RPC path"));
    }
    Ok(())
}

/// Builds Protobuf request headers with an optional caller-validated timeout value.
pub fn request_headers(
    metadata: &Metadata,
    timeout: Option<&str>,
    limit: usize,
) -> Result<HeaderMap> {
    let mut headers = metadata.headers.clone();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/grpc+proto"),
    );
    headers.insert("te", HeaderValue::from_static("trailers"));
    headers.insert("grpc-accept-encoding", HeaderValue::from_static("identity"));
    if let Some(timeout) = timeout {
        headers.insert(
            "grpc-timeout",
            HeaderValue::from_str(timeout).map_err(|_| Error::InvalidHeader("grpc-timeout"))?,
        );
    }
    check_headers(&headers, limit)?;
    Ok(headers)
}

/// Validates request fields and borrows the optional raw timeout for the caller to parse.
pub fn validate_request<'a>(
    method: &Method,
    path: &str,
    headers: &'a HeaderMap,
    limit: usize,
) -> Result<Option<&'a str>> {
    check_headers(headers, limit)?;
    if method != Method::POST {
        return Err(Error::InvalidHeader("HTTP method"));
    }
    validate_path(path)?;
    validate_content_type(headers)?;
    if one(headers, "te")? != Some("trailers") {
        return Err(Error::InvalidHeader("te"));
    }
    validate_encoding(headers)?;
    one(headers, "grpc-timeout")
}

/// Accepts the native Protobuf gRPC media types, including optional parameters.
pub fn validate_content_type(headers: &HeaderMap) -> Result<()> {
    let value = one(headers, "content-type")?.ok_or(Error::InvalidHeader("content-type"))?;
    let media_type = value.split(';').next().map(str::trim);
    if !matches!(
        media_type,
        Some("application/grpc" | "application/grpc+proto")
    ) {
        return Err(Error::InvalidHeader("content-type"));
    }
    Ok(())
}

/// Rejects unsupported message encodings and duplicate encoding fields.
pub fn validate_encoding(headers: &HeaderMap) -> Result<()> {
    if one(headers, "grpc-encoding")?.is_some_and(|value| value != "identity") {
        return Err(Error::UnsupportedCompression);
    }
    Ok(())
}

/// Builds leading Protobuf response headers, preserving application metadata.
pub fn response_headers(metadata: &Metadata, limit: usize) -> Result<HeaderMap> {
    let mut headers = metadata.headers.clone();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/grpc+proto"),
    );
    check_response_headers(&headers, limit)?;
    Ok(headers)
}

/// Builds final status trailers with application metadata and optional rich details.
pub fn status_headers(status: &Status, metadata: &Metadata, limit: usize) -> Result<HeaderMap> {
    validate_status_details(status.code(), status.details())?;
    let mut headers = metadata.headers.clone();
    headers.insert(
        "grpc-status",
        HeaderValue::from_str(&(status.code() as u8).to_string())
            .map_err(|_| Error::InvalidHeader("grpc-status"))?,
    );
    headers.insert(
        "grpc-message",
        HeaderValue::from_str(&encode_message(status.message()))
            .map_err(|_| Error::InvalidHeader("grpc-message"))?,
    );
    if !status.details().is_empty() {
        headers.insert(
            "grpc-status-details-bin",
            HeaderValue::from_str(&STANDARD_NO_PAD.encode(status.details()))
                .map_err(|_| Error::InvalidHeader("grpc-status-details-bin"))?,
        );
    }
    check_headers(&headers, limit)?;
    Ok(headers)
}

/// Parses required final status fields from trailers or a trailers-only header list.
pub fn parse_status(headers: &HeaderMap, limit: usize) -> Result<Status> {
    check_headers(headers, limit)?;
    let code = one(headers, "grpc-status")?.ok_or(Error::MissingStatus)?;
    let code = Code::parse(code).unwrap_or(Code::Unknown);
    let message = one(headers, "grpc-message")?
        .map(decode_message)
        .unwrap_or_default();
    let status = Status::new(code, &message);
    if let Some(details) = one(headers, "grpc-status-details-bin")? {
        let details = decode_binary(details.as_bytes())?;
        return status.with_details(&details);
    }
    Ok(status)
}

/// Maps an HTTP error response to its fallback RPC status.
pub fn http_error(status: StatusCode) -> Status {
    let code = match status.as_u16() {
        400 => Code::Internal,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::Unimplemented,
        429 | 502 | 503 | 504 => Code::Unavailable,
        _ => Code::Unknown,
    };
    Status::new(code, &format!("HTTP status {status}"))
}

fn one<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Error::InvalidHeader("duplicate protocol header"));
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| Error::InvalidHeader("non-ASCII protocol header"))
}

fn encode_message(message: &str) -> String {
    let mut encoded = String::new();
    for byte in message.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    encoded
}

fn decode_message(message: &str) -> String {
    let mut decoded = Vec::new();
    let mut bytes = message.bytes();
    while let Some(byte) = bytes.next() {
        if byte != b'%' {
            decoded.push(byte);
            continue;
        }
        let high = bytes.next().and_then(|b| char::from(b).to_digit(16));
        let low = bytes.next().and_then(|b| char::from(b).to_digit(16));
        let (Some(high), Some(low)) = (high, low) else {
            return message.to_owned();
        };
        decoded.push((high * 16 + low) as u8);
    }
    String::from_utf8(decoded).unwrap_or_else(|_| message.to_owned())
}

#[derive(prost::Message)]
struct StatusDetails {
    #[prost(int32, optional, tag = "1")]
    code: Option<i32>,
}

pub(crate) fn validate_status_details(code: Code, details: &[u8]) -> Result<()> {
    if details.is_empty() {
        return Ok(());
    }
    if code == Code::Ok {
        return Err(Error::InvalidHeader("rich details on OK status"));
    }
    let details = <StatusDetails as prost::Message>::decode(details)?;
    if details
        .code
        .is_some_and(|value| value != i32::from(code as u8))
    {
        return Err(Error::InvalidHeader("rich status contradicts grpc-status"));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn fixed_status_vector_and_all_codes() {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", HeaderValue::from_static("7"));
        headers.insert(
            "grpc-message",
            HeaderValue::from_static("denied%3A%20%E6%B5%8B%E8%AF%95%20%25"),
        );
        headers.insert("grpc-status-details-bin", HeaderValue::from_static("CAc="));
        let status = parse_status(&headers, 8192).unwrap();
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), "denied: 测试 %");
        assert_eq!(status.details(), &[8, 7]);
        let encoded = status_headers(&status, &Metadata::new(), 8192).unwrap();
        assert_eq!(encoded["grpc-status"], "7");
        assert_eq!(encoded["grpc-message"], "denied: %E6%B5%8B%E8%AF%95 %25");
        assert_eq!(encoded["grpc-status-details-bin"], "CAc");
        for number in 0..=16 {
            assert!(Code::parse(&number.to_string()).is_some());
        }
        assert_eq!(
            parse_status(&HeaderMap::new(), 8192)
                .unwrap_err()
                .to_string(),
            Error::MissingStatus.to_string()
        );
    }

    #[test]
    fn binary_metadata_accepts_comma_joined_padded_and_unpadded_values() {
        let mut headers = HeaderMap::new();
        headers.append("custom-bin", HeaderValue::from_static("AQI=, /w"));
        headers.append("custom-bin", HeaderValue::from_static("AA=="));
        let metadata = Metadata::from_headers(&headers).unwrap();
        assert_eq!(
            metadata.binary_values("custom-bin").unwrap(),
            [vec![1, 2], vec![255], vec![0]]
        );
        headers.append("custom-bin", HeaderValue::from_static("!"));
        assert!(Metadata::from_headers(&headers).is_err());
    }

    #[test]
    fn protocol_fields_are_reserved_and_metadata_names_are_lowercase() {
        let mut metadata = Metadata::new();
        for key in [
            "grpc-status",
            "te",
            "content-type",
            "Upper",
            "",
            "invalid space",
            ":authority",
        ] {
            assert!(metadata.append_ascii(key, "x").is_err(), "{key}");
        }
        assert!(metadata.append_ascii("x-bin", "binary").is_err());
        assert!(metadata.append_binary("x", &[1]).is_err());
        assert!(metadata.append_ascii("x", "\n").is_err());
        metadata.append_ascii("a_b.c-1", "one").unwrap();
        metadata.append_ascii("a_b.c-1", "two").unwrap();
        assert_eq!(
            metadata.ascii_values("a_b.c-1").collect::<Vec<_>>(),
            ["one", "two"]
        );
    }

    #[test]
    fn headers_limits_duplicate_status_and_malformed_messages() {
        let mut headers = HeaderMap::new();
        headers.insert("x", HeaderValue::from_static("ab"));
        check_headers(&headers, 35).unwrap();
        assert!(matches!(
            check_headers(&headers, 34),
            Err(Error::LimitExceeded { size: 35, .. })
        ));
        headers.append("grpc-status", HeaderValue::from_static("0"));
        headers.append("grpc-status", HeaderValue::from_static("7"));
        assert!(parse_status(&headers, 8192).is_err());
        for message in ["%", "%0", "%GG", "%FF"] {
            assert_eq!(decode_message(message), message);
        }
        assert!(validate_status_details(Code::Ok, &[8, 0]).is_err());
        assert!(validate_status_details(Code::PermissionDenied, &[8, 3]).is_err());
        validate_status_details(Code::PermissionDenied, &[8, 7]).unwrap();
    }

    #[test]
    fn header_limits_include_http2_pseudo_fields() {
        let request = http::Request::builder()
            .method("POST")
            .uri("http://foo/s/m")
            .header("x", "ab")
            .body(())
            .unwrap();
        check_request_headers(&request, 207).unwrap();
        assert!(matches!(
            check_request_headers(&request, 206),
            Err(Error::LimitExceeded {
                size: 207,
                limit: 206,
                ..
            })
        ));
        let mut headers = HeaderMap::new();
        headers.insert("x", HeaderValue::from_static("ab"));
        check_response_headers(&headers, 77).unwrap();
        assert!(matches!(
            check_response_headers(&headers, 76),
            Err(Error::LimitExceeded {
                size: 77,
                limit: 76,
                ..
            })
        ));
    }

    #[test]
    fn paths_media_types_encoding_and_http_mapping() {
        for path in ["/test.Echo/Unary", "/test.Echo/unary"] {
            validate_path(path).unwrap();
        }
        for path in [
            "test.Echo/Unary",
            "/test.Echo/",
            "//Unary",
            "/test.Echo/Unary/extra",
            "/test.Echo/Unary?x",
        ] {
            assert!(validate_path(path).is_err());
        }
        let mut headers = request_headers(&Metadata::new(), None, 8192).unwrap();
        assert!(validate_request(&Method::GET, "/s/m", &headers, 8192).is_err());
        for value in [
            "application/grpc",
            "application/grpc+proto",
            "application/grpc+proto; charset=utf-8",
        ] {
            headers.insert("content-type", HeaderValue::from_str(value).unwrap());
            validate_content_type(&headers).unwrap();
        }
        for value in ["text/plain", "application/grpc+json", "application/grpcxyz"] {
            headers.insert("content-type", HeaderValue::from_str(value).unwrap());
            assert!(validate_content_type(&headers).is_err());
        }
        headers.insert("grpc-encoding", HeaderValue::from_static("gzip"));
        assert!(matches!(
            validate_encoding(&headers),
            Err(Error::UnsupportedCompression)
        ));
        assert_eq!(
            http_error(StatusCode::NOT_FOUND).code(),
            Code::Unimplemented
        );
        assert_eq!(
            http_error(StatusCode::SERVICE_UNAVAILABLE).code(),
            Code::Unavailable
        );
    }
}
