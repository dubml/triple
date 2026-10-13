//! Triple HTTP RPC Unary binding for a caller-owned hyper server.
//!
//! Business handlers receive one decoded message and shared RPC semantics.
//! This crate does not own sockets, listeners or Protobuf message definitions.
#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod metadata;
mod request;
mod response;

use std::future::Future;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use triple_core::{Deadline, Metadata, Path, Status};

pub use metadata::is_reserved_header;
pub use response::to_http;

/// The bare HTTP message codec, independent of message schemas.
/// Handlers must return an error for unknown variants.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Encoding {
    /// A single JSON value extracted from a one-element request array.
    Json,
    /// Bare Protobuf bytes, with no gRPC frame or wrapper.
    ///
    /// TODO(spec): An extra HTTP proto envelope is not defined in the specification;
    /// forward generated-message bytes unchanged as agreed for this binding.
    Proto,
}

impl Encoding {
    pub(crate) fn media_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Proto => "application/proto",
        }
    }
}

/// Shared call semantics and received Triple fields.
#[derive(Debug)]
#[non_exhaustive]
pub struct CallContext {
    /// Absolute monotonic deadline; absence means unlimited time.
    pub deadline: Option<Deadline>,
    /// Decoded, valid application metadata.
    pub metadata: Metadata,
    /// Recorded verbatim; its grammar and required presence remain unspecified.
    pub protocol_version: Option<Bytes>,
    /// Opaque ASCII service version, without interpretation or normalization.
    pub service_version: Option<String>,
    /// Opaque ASCII service group, without interpretation or normalization.
    pub service_group: Option<String>,
    /// One diagnostic name per discarded metadata entry; never sent on the wire.
    pub discarded_metadata: Vec<String>,
}

/// Application routing and asynchronous Unary message handling.
pub trait UnaryHandler: Send + Sync {
    /// Reports whether this exact, case-sensitive service/method is registered.
    fn is_registered(&self, path: Path<'_>) -> bool;

    /// Handles decoded bytes; JSON input and output are single JSON values.
    fn call(
        &self,
        path: Path<'_>,
        encoding: Encoding,
        message: Bytes,
        context: CallContext,
    ) -> impl Future<Output = Result<Bytes, Status>> + Send;
}

/// Encoded and decoded request bodies independently obey this byte limit.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub struct Config {
    /// Maximum request size before and after decompression, inclusive.
    pub max_request_bytes: usize,
}

impl Config {
    /// Creates the default configuration with a 4 MiB request limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the inclusive limit for both encoded and decompressed request bytes.
    pub fn with_max_request_bytes(mut self, max_request_bytes: usize) -> Self {
        self.max_request_bytes = max_request_bytes;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_request_bytes: 4 * 1024 * 1024,
        }
    }
}

/// A stateless HTTP adapter to attach to a caller-owned hyper service.
#[derive(Debug, Default, Copy, Clone)]
pub struct Binding {
    config: Config,
}

impl Binding {
    /// Creates an adapter with an explicit request size limit.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Converts one real hyper request to one Unary response.
    pub async fn handle<H: UnaryHandler>(
        &self,
        request: Request<Incoming>,
        handler: &H,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = request.into_parts();
        let prepared = request::prepare(&parts, handler);
        let (path, encoding, compression, context) = match prepared {
            Ok(prepared) => prepared,
            Err(status) => return response::error(status),
        };
        let deadline = context.deadline;
        let result = enforce_deadline(deadline, async {
            let bytes =
                request::read_message(body, compression, self.config.max_request_bytes).await?;
            let message = request::decode_message(encoding, bytes)?;
            let bytes = handler
                .call(path, encoding, message, context)
                .await
                .map_err(|status| to_http(status.code()))?;
            if encoding == Encoding::Json
                && serde_json::from_slice::<&serde_json::value::RawValue>(&bytes).is_err()
            {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            Ok(bytes)
        })
        .await;
        match result {
            Ok(bytes) => response::success(encoding, bytes),
            Err(status) => response::error(status),
        }
    }
}

// TODO: Merge this single deadline enforcement helper with rpc after its hyper migration.
async fn enforce_deadline<T>(
    deadline: Option<Deadline>,
    future: impl Future<Output = Result<T, StatusCode>>,
) -> Result<T, StatusCode> {
    let Some(deadline) = deadline else {
        return future.await;
    };
    if deadline.is_expired_at(std::time::Instant::now()) {
        return Err(StatusCode::REQUEST_TIMEOUT);
    }
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline.instant()), future)
        .await
        .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
}
