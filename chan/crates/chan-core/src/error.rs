//! Error type for `chan-core`.

use thiserror::Error;

/// Every fallible operation in `chan-core` returns this.
#[derive(Debug, Error)]
pub enum ChanError {
    /// Underlying filesystem I/O failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// (De)serialization of a message or state file failed.
    #[error("serialize: {0}")]
    Serde(#[from] serde_json::Error),
    /// Acquiring or releasing a file lock failed.
    #[error("lock: {0}")]
    Lock(String),
    /// A participant, room, or message was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// A cursor pointed outside the log.
    #[error("cursor out of range: {0}")]
    Cursor(String),
    /// A message outlived its TTL.
    #[error("expired: {0}")]
    TtlExpired(String),
    /// The caller violated the channel protocol.
    #[error("protocol: {0}")]
    Protocol(String),
    /// A network transport to a remote channel failed.
    #[error("transport: {0}")]
    Transport(String),
}

/// Convenience alias for `Result<T, ChanError>`.
pub type Result<T> = std::result::Result<T, ChanError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_mentions_context() {
        let e = ChanError::NotFound("room:build".into());
        assert!(e.to_string().contains("room:build"));
    }
}
