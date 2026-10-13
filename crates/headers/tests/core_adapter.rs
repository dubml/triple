//! Existing wire adapters share semantic types without moving wire formats into the core.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use headers::{Metadata, Status, parse_status, request_headers, status_headers};
use http::{HeaderMap, HeaderValue};
use triple_core::Code;

#[test]
fn metadata_moves_to_binding_and_round_trips_decoded_values() {
    let mut semantic = triple_core::Metadata::new();
    semantic.append_ascii("echo", "first").unwrap();
    semantic.append_ascii("echo", "second").unwrap();
    semantic.append_binary("echo-bin", &[0, 255]).unwrap();
    let binding = Metadata::from(semantic);
    let fields = request_headers(&binding, None, 8192).unwrap();
    assert_eq!(fields["echo-bin"], "AP8");
    let received = Metadata::from_headers(&fields).unwrap();
    assert_eq!(binding.as_ref(), received.as_ref());
}

#[test]
fn peer_base64_is_decoded_once_before_entering_core() {
    let mut fields = HeaderMap::new();
    fields.append("echo-bin", HeaderValue::from_static("AA==, /w"));
    fields.append("echo-bin", HeaderValue::from_static(""));
    let binding = Metadata::from_headers(&fields).unwrap();
    assert_eq!(
        binding
            .as_ref()
            .binary_values("echo-bin")
            .collect::<Vec<_>>(),
        [&[0][..], &[255][..], &[][..]]
    );
    let encoded = request_headers(&binding, None, 8192).unwrap();
    assert_eq!(
        encoded
            .get_all("echo-bin")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect::<Vec<_>>(),
        [b"AA".as_slice(), b"/w", b""]
    );
}

#[test]
fn reserved_header_policy_is_enforced_by_binding() {
    let mut semantic = triple_core::Metadata::new();
    semantic.append_ascii("grpc-status", "0").unwrap();
    let binding = Metadata::from(semantic);
    assert!(request_headers(&binding, None, 8192).is_err());
}

#[test]
fn status_keeps_unicode_in_core_and_percent_encoding_in_binding() {
    let binding = Status::from(triple_core::Status::new(Code::PermissionDenied, "拒绝 %"));
    let fields = status_headers(&binding, &Metadata::new(), 8192).unwrap();
    assert_eq!(fields["grpc-status"], "7");
    assert_eq!(fields["grpc-message"], "%E6%8B%92%E7%BB%9D %25");
    let received = parse_status(&fields, 8192).unwrap();
    assert_eq!(binding.as_ref(), received.as_ref());
}

#[test]
fn malformed_wire_status_values_remain_unknown() {
    for value in ["", "00", "01", "+1", "-1", " 1", "1 ", "17", "256", "x"] {
        let mut fields = HeaderMap::new();
        fields.insert("grpc-status", HeaderValue::from_str(value).unwrap());
        assert_eq!(
            parse_status(&fields, 8192).unwrap().code(),
            Code::Unknown,
            "{value}"
        );
    }
}
