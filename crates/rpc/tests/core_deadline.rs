//! Conversion between the gRPC timeout adapter and the shared monotonic deadline.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

#[test]
fn binding_conversion_preserves_the_absolute_budget() {
    let semantic = triple_core::Deadline::after_at(Duration::from_secs(3), Instant::now()).unwrap();
    let binding = rpc::Deadline::from(semantic);
    let restored = triple_core::Deadline::from(binding);
    assert_eq!(restored, semantic);
    assert_eq!(restored.instant(), semantic.instant());
    assert!(binding.to_header().is_ok());
}
