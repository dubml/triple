//! Shared Triple RPC semantics, independent of HTTP libraries and message framing.
//!
//! Bindings translate wire fields into these types. Metadata stores decoded values;
//! deadlines use monotonic time, with `Option<Deadline>::None` representing no deadline.

mod deadline;
mod error;
mod metadata;
mod path;
mod status;

pub use deadline::Deadline;
pub use error::{Error, Result};
pub use metadata::{Metadata, MetadataValue};
pub use path::Path;
pub use status::{Code, Status};
