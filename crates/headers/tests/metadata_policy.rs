//! Caller-selected reserved fields and the unchanged gRPC invalid Base64 policy.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use http::{HeaderMap, HeaderValue};
use triple_headers::{Error, Metadata, is_grpc_reserved_header};

#[test]
fn grpc_invalid_binary_metadata_fails_the_whole_parse() {
    let mut fields = HeaderMap::new();
    fields.insert("valid", HeaderValue::from_static("kept"));
    fields.insert("invalid-bin", HeaderValue::from_static("!"));
    assert!(matches!(
        Metadata::from_headers_with_reserved(&fields, is_grpc_reserved_header),
        Err(Error::InvalidHeader("binary metadata Base64"))
    ));
    assert!(Metadata::from_headers(&fields).is_err());
}

#[test]
fn caller_predicate_controls_reserved_names_for_ascii_and_binary_metadata() {
    let mut fields = HeaderMap::new();
    fields.insert("grpc-custom", HeaderValue::from_static("application"));
    fields.insert("grpc-custom-bin", HeaderValue::from_static("AA=="));
    fields.insert("reserved", HeaderValue::from_static("hidden"));
    let metadata =
        Metadata::from_headers_with_reserved(&fields, |name| name == "reserved").unwrap();
    assert_eq!(
        metadata.ascii_values("grpc-custom").collect::<Vec<_>>(),
        ["application"]
    );
    assert_eq!(
        metadata.binary_values("grpc-custom-bin").unwrap(),
        [vec![0]]
    );
    assert!(metadata.ascii_values("reserved").next().is_none());
}
