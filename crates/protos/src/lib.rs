//! Shared Protobuf contracts and generated bindings for tests and examples.
//!
//! These fixtures describe Echo and Greeter services, not the Triple wire protocol.
#![forbid(unsafe_code)]

/// Echo messages and client/server bindings for interoperability fixtures.
#[allow(missing_docs)]
pub mod echo {
    tonic::include_proto!("test");
}

/// Greeter messages and client/server bindings for HTTP and RPC fixtures.
#[allow(missing_docs)]
pub mod greeter {
    tonic::include_proto!("demo");
}
