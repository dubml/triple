//! Shared Triple RPC semantics, independent of HTTP libraries and message framing.
//!
//! Bindings translate wire fields into these types. Metadata stores decoded values;
//! deadlines use monotonic time, with `Option<Deadline>::None` representing no deadline.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod deadline;
mod error;
mod metadata;
mod path;
mod status;
mod timeout;

pub use deadline::Deadline;
pub use error::{Error, Result};
pub use metadata::{Metadata, MetadataValue};
pub use path::Path;
pub use status::{Code, Status};
pub use timeout::{parse_grpc_timeout, parse_tri_service_timeout};
