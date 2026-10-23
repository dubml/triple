//! Existing wire adapters share semantic types without moving wire formats into the core.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use http::{HeaderMap, HeaderValue};
use triple_core::Code;
use triple_headers::{Metadata, Status, parse_status, request_headers, status_headers};

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
fn binary_metadata_decodes_on_receive_accepts_padded_and_unpadded() {
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
fn grpc_reserved_names_are_filtered_from_custom_metadata() {
    let mut fields = HeaderMap::new();
    fields.insert("grpc-timeout", HeaderValue::from_static("1S"));
    fields.insert("grpc-status", HeaderValue::from_static("0"));
    fields.insert("grpc-future-bin", HeaderValue::from_static("!"));
    fields.insert("custom", HeaderValue::from_static("kept"));
    let metadata = Metadata::from_headers(&fields).unwrap();
    assert_eq!(metadata.as_ref().iter().count(), 1);
    assert_eq!(
        metadata.ascii_values("custom").collect::<Vec<_>>(),
        ["kept"]
    );
    assert!(metadata.ascii_values("grpc-timeout").next().is_none());
    assert!(metadata.ascii_values("grpc-status").next().is_none());
    assert!(
        metadata
            .as_ref()
            .binary_values("grpc-future-bin")
            .next()
            .is_none()
    );
}

#[test]
fn invalid_ascii_metadata_is_discarded_and_recorded_without_request_failure() {
    for (name, invalid) in [
        ("horizontal_tab", HeaderValue::from_static("bad\tvalue")),
        ("non_ascii_octet", HeaderValue::from_bytes(&[255]).unwrap()),
    ] {
        let mut fields = HeaderMap::new();
        fields.append("custom", HeaderValue::from_static("before"));
        fields.append("custom", invalid);
        fields.append("custom", HeaderValue::from_static("after"));
        let metadata = Metadata::from_headers(&fields).unwrap();
        assert_eq!(
            metadata.ascii_values("custom").collect::<Vec<_>>(),
            ["before", "after"],
            "{name}"
        );
        assert_eq!(
            metadata.discarded_ascii_names().collect::<Vec<_>>(),
            ["custom"],
            "{name}"
        );
        let encoded = request_headers(&metadata, None, 8192).unwrap();
        assert_eq!(encoded.get_all("custom").iter().count(), 2, "{name}");
    }
}

#[test]
fn timeout_helpers_delegate_to_shared_core_parsers() {
    for input in ["1", "3000", "0", "-1", ""] {
        assert_eq!(
            triple_headers::parse_tri_service_timeout(input),
            triple_core::parse_tri_service_timeout(input),
            "tri: {input}"
        );
    }
    for input in ["1H", "1M", "1S", "1m", "1u", "1n", "0n", "1X", ""] {
        assert_eq!(
            triple_headers::parse_grpc_timeout(input),
            triple_core::parse_grpc_timeout(input),
            "grpc: {input}"
        );
    }
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
