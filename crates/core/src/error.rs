/// Errors in shared RPC semantic values, without transport-specific error fields.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The path does not contain exactly one service and one method.
    #[error("invalid RPC path")]
    InvalidPath,
    /// A metadata name violates the shared lowercase name grammar.
    #[error("invalid metadata name")]
    InvalidMetadataName,
    /// An ASCII metadata value contains non-printable or non-ASCII bytes.
    #[error("invalid ASCII metadata value")]
    InvalidAsciiValue,
    /// A binary metadata name does not end in `-bin`.
    #[error("invalid binary metadata name")]
    InvalidBinaryName,
    /// The timeout cannot be represented as a monotonic deadline.
    #[error("deadline overflow")]
    DeadlineOverflow,
    /// The numeric status does not identify a standard RPC code.
    #[error("invalid RPC status code: {0}")]
    InvalidCode(u8),
}

/// Result of constructing a shared semantic value.
pub type Result<T> = std::result::Result<T, Error>;
