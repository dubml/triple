use std::io::Read;

use bytes::Bytes;
use flate2::read::MultiGzDecoder;
use http::{HeaderMap, HeaderValue, Method, StatusCode, request::Parts};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use serde_json::value::RawValue;
use triple_core::{Deadline, Path, parse_tri_service_timeout};

use crate::{CallContext, Encoding, UnaryHandler, metadata};

pub(crate) type Prepared<'a> = (Path<'a>, Encoding, Compression, CallContext);

#[derive(Copy, Clone)]
pub(crate) enum Compression {
    Identity,
    Gzip,
}

pub(crate) fn prepare<'a>(
    parts: &'a Parts,
    handler: &impl UnaryHandler,
) -> Result<Prepared<'a>, StatusCode> {
    if parts.method != Method::POST {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    // TODO(spec): Query strings have no specified HTTP RPC semantics; reject them.
    if parts.uri.query().is_some() {
        return Err(StatusCode::NOT_FOUND);
    }
    let path = Path::parse(parts.uri.path()).map_err(|_| StatusCode::NOT_FOUND)?;
    if !handler.is_registered(path) {
        return Err(StatusCode::NOT_FOUND);
    }
    let encoding = match single_value(&parts.headers, "content-type")?
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
    {
        Some("application/json") => Encoding::Json,
        Some("application/proto") => Encoding::Proto,
        _ => return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE),
    };
    let content_encoding = single_value(&parts.headers, "content-encoding")?
        .map(|value| {
            value
                .to_str()
                .map_err(|_| StatusCode::UNSUPPORTED_MEDIA_TYPE)
        })
        .transpose()?;
    let compression = match content_encoding {
        None | Some("identity") => Compression::Identity,
        Some("gzip") => Compression::Gzip,
        // TODO(spec): The status for unsupported Content-Encoding is unspecified.
        // This phase supports identity/gzip and uses 415 for other values.
        _ => return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE),
    };
    let deadline = single_ascii(&parts.headers, "tri-service-timeout")?
        .map(|value| {
            let duration = parse_tri_service_timeout(value).map_err(|_| StatusCode::BAD_REQUEST)?;
            Deadline::after_at(duration, std::time::Instant::now())
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()?;
    // TODO(spec): Version grammar/validation and mandatory presence remain unknown.
    let protocol_version = single_value(&parts.headers, "tri-protocol-version")?
        .map(|value| Bytes::copy_from_slice(value.as_bytes()));
    let service_version = single_ascii(&parts.headers, "tri-service-version")?.map(str::to_owned);
    let service_group = single_ascii(&parts.headers, "tri-service-group")?.map(str::to_owned);
    let (metadata, discarded_metadata) = metadata::parse(&parts.headers);
    Ok((
        path,
        encoding,
        compression,
        CallContext {
            deadline,
            metadata,
            protocol_version,
            service_version,
            service_group,
            discarded_metadata,
        },
    ))
}

fn single_value<'a>(
    headers: &'a HeaderMap,
    name: &str,
) -> Result<Option<&'a HeaderValue>, StatusCode> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(first)
}

fn single_ascii<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, StatusCode> {
    single_value(headers, name)?
        .map(|value| value.to_str().map_err(|_| StatusCode::BAD_REQUEST))
        .transpose()
}

pub(crate) async fn read_message(
    mut body: Incoming,
    compression: Compression,
    limit: usize,
) -> Result<Bytes, StatusCode> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| StatusCode::BAD_REQUEST)?;
        if let Ok(data) = frame.into_data() {
            append_bounded(&mut bytes, &data, limit)?;
        }
    }
    match compression {
        Compression::Identity => Ok(Bytes::from(bytes)),
        Compression::Gzip => {
            let mut decoder = MultiGzDecoder::new(bytes.as_slice());
            let mut decoded = Vec::new();
            let mut chunk = [0; 8192];
            loop {
                let count = decoder
                    .read(&mut chunk)
                    .map_err(|_| StatusCode::BAD_REQUEST)?;
                if count == 0 {
                    break;
                }
                append_bounded(&mut decoded, &chunk[..count], limit)?;
            }
            Ok(Bytes::from(decoded))
        }
    }
}

fn append_bounded(bytes: &mut Vec<u8>, data: &[u8], limit: usize) -> Result<(), StatusCode> {
    if data.len() > limit.saturating_sub(bytes.len()) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    bytes.extend_from_slice(data);
    Ok(())
}

pub(crate) fn decode_message(encoding: Encoding, bytes: Bytes) -> Result<Bytes, StatusCode> {
    if encoding == Encoding::Proto {
        return Ok(bytes);
    }
    // TODO(spec): Multiple elements represent Java interface parameters, which this
    // IDL-only Unary binding does not implement. Exactly one element is accepted.
    let [element]: [&RawValue; 1] =
        serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Bytes::copy_from_slice(element.get().as_bytes()))
}
