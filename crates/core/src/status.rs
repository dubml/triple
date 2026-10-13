use std::fmt;

use crate::{Error, Result};

/// Shared RPC status code; numeric values match the gRPC status registry.
///
/// This enum remains exhaustive because gRPC defines it as a closed set of codes.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Code {
    /// Successful completion.
    Ok = 0,
    /// Caller cancelled the operation.
    Cancelled = 1,
    /// Unknown failure.
    Unknown = 2,
    /// Invalid argument.
    InvalidArgument = 3,
    /// Deadline expired.
    DeadlineExceeded = 4,
    /// Resource not found.
    NotFound = 5,
    /// Resource already exists.
    AlreadyExists = 6,
    /// Access denied.
    PermissionDenied = 7,
    /// Resource limit reached.
    ResourceExhausted = 8,
    /// Required precondition failed.
    FailedPrecondition = 9,
    /// Operation aborted.
    Aborted = 10,
    /// Argument outside range.
    OutOfRange = 11,
    /// Method or feature unavailable.
    Unimplemented = 12,
    /// Internal failure.
    Internal = 13,
    /// Endpoint temporarily unavailable.
    Unavailable = 14,
    /// Irrecoverable data loss.
    DataLoss = 15,
    /// Invalid or missing credentials.
    Unauthenticated = 16,
}

impl Code {
    /// Maps only non-200 HTTP responses using the confirmed Triple HTTP RPC mapping.
    ///
    /// Triple specification §3.1.3, HTTP Status / Inferred Code table:
    /// <https://dubbo.apache.org/zh-cn/overview/reference/protocols/triple-spec/>.
    /// A 200 response is handled by the binding before applying this fallback.
    pub fn from_http(status: u16) -> Self {
        match status {
            400 => Self::InvalidArgument,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 => Self::Unimplemented,
            408 => Self::DeadlineExceeded,
            409 => Self::Aborted,
            412 => Self::FailedPrecondition,
            413 | 431 => Self::ResourceExhausted,
            415 => Self::Internal,
            429 | 502 | 503 | 504 => Self::Unavailable,
            _ => Self::Unknown,
        }
    }
}

impl TryFrom<u8> for Code {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::Cancelled),
            2 => Ok(Self::Unknown),
            3 => Ok(Self::InvalidArgument),
            4 => Ok(Self::DeadlineExceeded),
            5 => Ok(Self::NotFound),
            6 => Ok(Self::AlreadyExists),
            7 => Ok(Self::PermissionDenied),
            8 => Ok(Self::ResourceExhausted),
            9 => Ok(Self::FailedPrecondition),
            10 => Ok(Self::Aborted),
            11 => Ok(Self::OutOfRange),
            12 => Ok(Self::Unimplemented),
            13 => Ok(Self::Internal),
            14 => Ok(Self::Unavailable),
            15 => Ok(Self::DataLoss),
            16 => Ok(Self::Unauthenticated),
            _ => Err(Error::InvalidCode(value)),
        }
    }
}

/// Final RPC status, independent of response headers and rich-error wire formats.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub struct Status {
    code: Code,
    message: String,
}

impl Status {
    /// Creates a status from a borrowed human-readable message.
    pub fn new(code: Code, message: &str) -> Self {
        Self {
            code,
            message: message.to_owned(),
        }
    }

    /// Creates the successful status.
    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    /// Returns the status code.
    pub fn code(&self) -> Code {
        self.code
    }

    /// Borrows the human-readable message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Status {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}
