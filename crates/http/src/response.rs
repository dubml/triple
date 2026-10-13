use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode, header};
use http_body_util::Full;
use triple_core::Code;

use crate::Encoding;

/// Converts handler codes using the explicitly agreed inverse HTTP mapping.
///
/// TODO(spec): The specification supplies only HTTP-to-RPC inference; this inverse
/// is a project decision. Codes without an agreed inverse use HTTP 500.
pub fn to_http(code: Code) -> StatusCode {
    match code {
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::Unimplemented => StatusCode::NOT_FOUND,
        Code::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        Code::Aborted => StatusCode::CONFLICT,
        Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::Ok
        | Code::Cancelled
        | Code::Unknown
        | Code::NotFound
        | Code::AlreadyExists
        | Code::ResourceExhausted
        | Code::OutOfRange
        | Code::Internal
        | Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn success(encoding: Encoding, bytes: Bytes) -> Response<Full<Bytes>> {
    let length = bytes.len();
    let mut response = Response::new(Full::new(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(encoding.media_type()),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    response
}

pub(crate) fn error(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
    }
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}
