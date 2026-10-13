use http::HeaderMap;
use triple_core::{Metadata, MetadataValue};

/// HTTP RPC fields and hop-by-hop fields excluded from application metadata.
pub fn is_reserved_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "content-type"
            | "content-length"
            | "content-encoding"
            | "accept"
            | "accept-encoding"
            | "user-agent"
            | "tri-protocol-version"
            | "tri-service-timeout"
            | "tri-service-version"
            | "tri-service-group"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

pub(crate) fn parse(fields: &HeaderMap) -> (Metadata, Vec<String>) {
    let mut metadata = Metadata::new();
    let mut discarded = Vec::new();
    for (name, value) in fields {
        if is_reserved_header(name.as_str()) {
            continue;
        }
        // TODO(spec): HTTP RPC does not define invalid custom metadata handling.
        // Discard and record each invalid entry as required for this binding.
        // TODO: HTTP RPC discards empty ASCII values while gRPC accepts them;
        // defer aligning the bindings until the agreed follow-up work.
        if !name.as_str().ends_with("-bin") && value.as_bytes().is_empty() {
            discarded.push(name.as_str().to_owned());
            continue;
        }
        let mut entry = HeaderMap::new();
        entry.append(name.clone(), value.clone());
        let parsed = match headers::Metadata::from_headers_with_reserved(&entry, is_reserved_header)
        {
            Ok(parsed) => parsed,
            Err(_) => {
                discarded.push(name.as_str().to_owned());
                continue;
            }
        };
        discarded.extend(parsed.discarded_ascii_names().map(str::to_owned));
        for (name, value) in parsed.as_ref().iter() {
            let result = match value {
                MetadataValue::Ascii(value) => metadata.append_ascii(name, value),
                MetadataValue::Binary(value) => metadata.append_binary(name, value),
            };
            if result.is_err() {
                discarded.push(name.to_owned());
            }
        }
    }
    (metadata, discarded)
}
