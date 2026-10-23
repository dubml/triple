//! Observes shutdown while the single-port server is still identifying HTTP.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../fake/common/mod.rs"]
mod demo;

use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn pre_cancelled_protocol_detection_returns_interrupted_without_draining() {
    let server = triple_server::Server::new(
        Arc::new(demo::Echo),
        Arc::new(demo::Greeter),
        triple_server::Config {
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    let (io, _peer) = tokio::io::duplex(64);
    let stop = triple_rpc::CancellationToken::new();
    stop.cancel();
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), server.serve_connection(io, stop))
        .await
        .unwrap();
    println!(
        "single-port pre-cancelled: budget_ms=1000 elapsed_ms={:.3} result={result:?}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(matches!(
        result,
        Err(triple_server::Error::Connection(error))
            if error.downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted)
    ));
}
