use std::time::Duration;

use crate::{Error, Result};

/// Parses a positive decimal `tri-service-timeout` in milliseconds.
///
/// No whitespace, sign or fallback timeout is accepted. Decimal overflow is an
/// error. The binding converts the result with `Deadline::after_at`.
pub fn parse_tri_service_timeout(value: &str) -> Result<Duration> {
    let count = decimal(value.as_bytes())?;
    if count == 0 {
        return Err(Error::InvalidTimeout);
    }
    Ok(Duration::from_millis(count))
}

/// Parses at most eight decimal digits followed by one of H, M, S, m, u or n.
///
/// Unit conversion uses checked arithmetic. The binding converts the returned
/// duration with `Deadline::after_at`, including zero as an expired deadline.
pub fn parse_grpc_timeout(value: &str) -> Result<Duration> {
    let (unit, digits) = value.as_bytes().split_last().ok_or(Error::InvalidTimeout)?;
    if digits.len() > 8 {
        return Err(Error::InvalidTimeout);
    }
    let count = decimal(digits)?;
    let multiplier = match unit {
        b'H' => Duration::from_secs(3600),
        b'M' => Duration::from_secs(60),
        b'S' => Duration::from_secs(1),
        b'm' => Duration::from_millis(1),
        b'u' => Duration::from_micros(1),
        b'n' => Duration::from_nanos(1),
        _ => return Err(Error::InvalidTimeoutUnit),
    };
    // TODO(spec): The grammar says positive, but tonic explicitly configured with
    // Duration::ZERO sends 0n (verified on the wire). Accept zero as expired.
    let count = u32::try_from(count).map_err(|_| Error::TimeoutOverflow)?;
    multiplier.checked_mul(count).ok_or(Error::TimeoutOverflow)
}

fn decimal(digits: &[u8]) -> Result<u64> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(Error::InvalidTimeout);
    }
    digits.iter().try_fold(0_u64, |count, digit| {
        count
            .checked_mul(10)
            .and_then(|count| count.checked_add(u64::from(digit - b'0')))
            .ok_or(Error::TimeoutOverflow)
    })
}
