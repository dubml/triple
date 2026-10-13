use std::time::{Duration, Instant};

use crate::{Error, Result};

/// Absolute monotonic deadline shared by all bindings.
///
/// Forwarding preserves this instant rather than restarting the budget.
/// `Option<Deadline>::None` represents an unlimited call.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Deadline(Instant);

impl Deadline {
    /// Creates a deadline from the current monotonic time with overflow checking.
    pub fn after(duration: Duration) -> Result<Self> {
        Self::after_at(duration, Instant::now())
    }

    /// The single duration-to-deadline conversion entry, using an explicit clock.
    ///
    /// Both timeout parsers return `Duration`; bindings call this entry once and
    /// forward the resulting absolute instant without restarting the budget.
    pub fn after_at(duration: Duration, now: Instant) -> Result<Self> {
        now.checked_add(duration)
            .map(Self)
            .ok_or(Error::DeadlineOverflow)
    }

    /// Wraps an absolute instant without resetting its budget.
    pub fn from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    /// Returns the absolute instant for a binding's timer.
    pub fn instant(self) -> Instant {
        self.0
    }

    /// Returns the remaining budget, or zero if the deadline has expired.
    pub fn remaining_at(self, now: Instant) -> Duration {
        self.0.saturating_duration_since(now)
    }

    /// Returns whether the deadline has been reached at the given clock reading.
    pub fn is_expired_at(self, now: Instant) -> bool {
        self.0 <= now
    }
}
