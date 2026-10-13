/// Errors in shared RPC semantic values, without transport-specific error fields.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
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
    /// A timeout does not satisfy its binding's decimal grammar.
    #[error("invalid timeout")]
    InvalidTimeout,
    /// A gRPC timeout unit is not H, M, S, m, u or n.
    #[error("invalid timeout unit")]
    InvalidTimeoutUnit,
    /// Decimal parsing or duration conversion overflowed.
    #[error("timeout overflow")]
    TimeoutOverflow,
    /// The numeric status does not identify a standard RPC code.
    #[error("invalid RPC status code: {0}")]
    InvalidCode(u8),
}

/// Result of constructing a shared semantic value.
pub type Result<T> = std::result::Result<T, Error>;
