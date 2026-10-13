//! Shared semantic contracts and the confirmed HTTP-to-RPC status table.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use triple_core::{Code, Deadline, Error, Metadata, MetadataValue, Path, Status};

#[test]
fn from_http_returns_unknown_for_500_and_418() {
    for status in [500, 418] {
        assert_eq!(Code::from_http(status), Code::Unknown, "HTTP {status}");
    }
}

#[test]
fn from_http_maps_all_14_confirmed_statuses() {
    for (http, expected) in [
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
    ] {
        assert_eq!(Code::from_http(http), expected, "HTTP {http}");
    }
}

#[test]
fn from_http_returns_unknown_for_every_unlisted_status() {
    let defined = [
        400, 401, 403, 404, 408, 409, 412, 413, 415, 429, 431, 502, 503, 504,
    ];
    for http in (u16::MIN..=u16::MAX).filter(|http| !defined.contains(http)) {
        assert_eq!(Code::from_http(http), Code::Unknown, "HTTP {http}");
    }
}

#[test]
fn standard_rpc_numeric_codes_and_unknown_numeric_values() {
    let codes = [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ];
    for (number, expected) in (0_u8..).zip(codes) {
        assert_eq!(expected as u8, number);
        assert_eq!(Code::try_from(number), Ok(expected));
    }
    for number in 17..=u8::MAX {
        assert_eq!(Code::try_from(number), Err(Error::InvalidCode(number)));
    }
}

#[test]
fn status_preserves_code_and_message_without_a_wire_format() {
    let status = Status::new(Code::PermissionDenied, "无访问权限 %");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "无访问权限 %");
    assert_eq!(status.to_string(), "PermissionDenied: 无访问权限 %");
    assert_eq!(Status::ok().code(), Code::Ok);
    assert!(Status::ok().message().is_empty());
}

#[test]
fn paths_borrow_service_and_method_without_case_normalization() {
    let simple = Path::parse("/svc/method").unwrap();
    assert_eq!((simple.service(), simple.method()), ("svc", "method"));
    let raw = String::from("/demo.GreetService/Greet");
    let path = Path::parse(&raw).unwrap();
    assert_eq!(path.as_str(), raw);
    assert_eq!(path.as_str().as_ptr(), raw.as_ptr());
    assert_eq!(path.service(), "demo.GreetService");
    assert_eq!(path.method(), "Greet");
    assert_ne!(path, Path::parse("/demo.GreetService/greet").unwrap());
    assert_ne!(path, Path::parse("/demo.greetservice/Greet").unwrap());
}

#[test]
fn path_rejects_named_invalid_cases() {
    for (name, path) in [
        ("empty_path", ""),
        ("only_separator", "/"),
        ("missing_leading_slash", "svc/method"),
        ("missing_method", "/svc"),
        ("empty_service", "//method"),
        ("empty_method", "/svc/"),
        ("trailing_separator", "/svc/method/"),
        ("extra_segment", "/svc/method/extra"),
        ("query", "/svc/method?query"),
        ("fragment", "/svc/method#fragment"),
    ] {
        assert_eq!(Path::parse(path), Err(Error::InvalidPath), "{name}: {path}");
    }
}

#[test]
fn metadata_preserves_duplicates_and_decoded_binary_values() {
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace-id", "first").unwrap();
    metadata
        .append_binary("payload-bin", &[0, 255, b','])
        .unwrap();
    metadata.append_ascii("trace-id", "second").unwrap();
    metadata.append_binary("payload-bin", &[]).unwrap();
    metadata.append_ascii("trace-id", "").unwrap();
    assert_eq!(
        metadata.ascii_values("trace-id").collect::<Vec<_>>(),
        ["first", "second", ""]
    );
    assert_eq!(
        metadata.binary_values("payload-bin").collect::<Vec<_>>(),
        [&[0, 255, b','][..], &[][..]]
    );
    let names = metadata.iter().map(|(name, _)| name).collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "trace-id",
            "payload-bin",
            "trace-id",
            "payload-bin",
            "trace-id"
        ]
    );
    let mut entries = metadata.iter();
    assert_eq!(
        entries.next(),
        Some(("trace-id", &MetadataValue::Ascii("first".into())))
    );
    assert_eq!(
        entries.next(),
        Some(("payload-bin", &MetadataValue::Binary(vec![0, 255, b','])))
    );
    assert!(metadata.ascii_values("missing").next().is_none());
    assert!(metadata.binary_values("missing-bin").next().is_none());
}

#[test]
fn invalid_metadata_does_not_change_existing_entries() {
    let mut metadata = Metadata::new();
    metadata.append_ascii("a_1.b-c", "kept").unwrap();
    for name in ["", "Trace", "space name", "name:", "中文"] {
        assert_eq!(
            metadata.append_ascii(name, "value"),
            Err(Error::InvalidMetadataName)
        );
        assert_eq!(
            metadata.append_binary(name, &[0]),
            Err(Error::InvalidMetadataName)
        );
    }
    for value in ["line\n", "tab\t", "\0", "é"] {
        assert_eq!(
            metadata.append_ascii("name", value),
            Err(Error::InvalidAsciiValue)
        );
    }
    assert_eq!(
        metadata.append_ascii("name-bin", "text"),
        Err(Error::InvalidAsciiValue)
    );
    assert_eq!(
        metadata.append_binary("name", &[0]),
        Err(Error::InvalidBinaryName)
    );
    assert_eq!(metadata.iter().count(), 1);
}

#[test]
fn core_does_not_decide_which_metadata_names_are_wire_headers() {
    let mut metadata = Metadata::new();
    metadata.append_ascii("grpc-timeout", "1S").unwrap();
    metadata.append_ascii("tri-service-version", "1").unwrap();
    assert_eq!(metadata.iter().count(), 2);
}

#[test]
fn deadline_budget_and_expiration_use_one_absolute_instant() {
    let now = Instant::now();
    let budget = Duration::from_millis(3000);
    let deadline = Deadline::after_at(budget, now).unwrap();
    let forwarded = Deadline::from_instant(deadline.instant());
    assert_eq!(forwarded, deadline);
    assert_eq!(deadline.remaining_at(now), budget);
    let later = now + Duration::from_millis(1000);
    assert_eq!(forwarded.remaining_at(later), Duration::from_millis(2000));
    assert!(!forwarded.is_expired_at(later));
    assert!(deadline.is_expired_at(deadline.instant()));
    assert_eq!(deadline.remaining_at(deadline.instant()), Duration::ZERO);
    assert_eq!(
        deadline.remaining_at(deadline.instant() + budget),
        Duration::ZERO
    );
    assert_eq!(
        Deadline::after_at(Duration::ZERO, now).unwrap().instant(),
        now
    );
}

#[test]
fn unrepresentable_deadline_returns_a_typed_error() {
    assert_eq!(
        Deadline::after_at(Duration::MAX, Instant::now()),
        Err(Error::DeadlineOverflow)
    );
}
