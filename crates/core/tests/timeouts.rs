//! Named table-driven rejection cases and shared duration/deadline conversion.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use triple_core::{Deadline, Error, parse_grpc_timeout, parse_tri_service_timeout};

macro_rules! rejection_cases {
    ($parser:path; $( $name:ident => ($input:literal, $error:path) ),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                assert_eq!($parser($input), Err($error), stringify!($name));
            }
        )+
    };
}

rejection_cases!(parse_tri_service_timeout;
    tri_service_timeout_rejects_zero => ("0", Error::InvalidTimeout),
    tri_service_timeout_rejects_negative => ("-1", Error::InvalidTimeout),
    tri_service_timeout_rejects_non_digit => ("abc", Error::InvalidTimeout),
    tri_service_timeout_rejects_empty => ("", Error::InvalidTimeout),
    tri_service_timeout_rejects_leading_whitespace => (" 1", Error::InvalidTimeout),
    tri_service_timeout_rejects_trailing_whitespace => ("1 ", Error::InvalidTimeout),
    tri_service_timeout_rejects_plus_sign => ("+1", Error::InvalidTimeout),
    tri_service_timeout_rejects_fraction => ("1.5", Error::InvalidTimeout),
    tri_service_timeout_rejects_unicode_digit => ("１", Error::InvalidTimeout),
    tri_service_timeout_rejects_overflow => ("18446744073709551616", Error::TimeoutOverflow),
);

rejection_cases!(parse_grpc_timeout;
    grpc_timeout_rejects_nine_digits => ("100000000S", Error::InvalidTimeout),
    grpc_timeout_rejects_nine_zero_digits => ("000000000n", Error::InvalidTimeout),
    grpc_timeout_rejects_unknown_unit => ("1X", Error::InvalidTimeoutUnit),
    grpc_timeout_rejects_missing_unit => ("12", Error::InvalidTimeoutUnit),
    grpc_timeout_rejects_missing_digits => ("S", Error::InvalidTimeout),
    grpc_timeout_rejects_empty => ("", Error::InvalidTimeout),
    grpc_timeout_rejects_negative => ("-1S", Error::InvalidTimeout),
    grpc_timeout_rejects_plus_sign => ("+1S", Error::InvalidTimeout),
    grpc_timeout_rejects_fraction => ("1.2S", Error::InvalidTimeout),
    grpc_timeout_rejects_leading_whitespace => (" 1S", Error::InvalidTimeout),
    grpc_timeout_rejects_trailing_whitespace => ("1S ", Error::InvalidTimeout),
    grpc_timeout_rejects_unicode_digit => ("１S", Error::InvalidTimeout),
);

#[test]
fn tri_service_timeout_parses_positive_milliseconds() {
    for (name, input, expected) in [
        ("one_millisecond", "1", Duration::from_millis(1)),
        ("three_seconds", "3000", Duration::from_secs(3)),
        ("leading_zero", "01", Duration::from_millis(1)),
        (
            "u64_boundary",
            "18446744073709551615",
            Duration::from_millis(u64::MAX),
        ),
    ] {
        assert_eq!(parse_tri_service_timeout(input), Ok(expected), "{name}");
    }
}

#[test]
fn grpc_timeout_converts_h_m_s_millis_micros_nanos_with_checked_arithmetic() {
    for (name, input, expected) in [
        ("hours", "2H", Duration::from_secs(7200)),
        ("minutes", "2M", Duration::from_secs(120)),
        ("seconds", "2S", Duration::from_secs(2)),
        ("milliseconds", "2m", Duration::from_millis(2)),
        ("microseconds", "2u", Duration::from_micros(2)),
        ("nanoseconds", "2n", Duration::from_nanos(2)),
        (
            "eight_digit_hours",
            "99999999H",
            Duration::from_secs(359_999_996_400),
        ),
        (
            "eight_digit_minutes",
            "99999999M",
            Duration::from_secs(5_999_999_940),
        ),
        (
            "eight_digit_seconds",
            "99999999S",
            Duration::from_secs(99_999_999),
        ),
        (
            "eight_digit_milliseconds",
            "99999999m",
            Duration::from_millis(99_999_999),
        ),
        (
            "eight_digit_microseconds",
            "99999999u",
            Duration::from_micros(99_999_999),
        ),
        (
            "eight_digit_nanoseconds",
            "99999999n",
            Duration::from_nanos(99_999_999),
        ),
    ] {
        assert_eq!(parse_grpc_timeout(input), Ok(expected), "{name}");
    }
}

#[test]
fn grpc_timeout_zero_is_accepted_and_already_expired_for_all_units() {
    let now = Instant::now();
    for input in ["0H", "0M", "0S", "0m", "0u", "0n"] {
        let duration = parse_grpc_timeout(input).unwrap();
        assert_eq!(duration, Duration::ZERO, "{input}");
        let deadline = Deadline::after_at(duration, now).unwrap();
        assert!(deadline.is_expired_at(now), "{input}");
    }
}

#[test]
fn both_timeout_parsers_share_one_duration_to_deadline_entry() {
    let now = Instant::now();
    let tri = Deadline::after_at(parse_tri_service_timeout("3000").unwrap(), now).unwrap();
    let grpc = Deadline::after_at(parse_grpc_timeout("3S").unwrap(), now).unwrap();
    assert_eq!(tri, grpc);
    assert_eq!(tri.instant(), now + Duration::from_secs(3));
}
