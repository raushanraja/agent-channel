//! The live-session port: opaque sessions, a stable control trait, a degrade signal.

use std::future::Future;
use std::pin::Pin;
use thiserror::Error;

/// A boxed, `Send` future. Lets [`Backend`] stay object-safe (`dyn Backend`) without an
/// `async-trait` dependency.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An opaque handle to a live session owned by some engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionRef(String);

impl SessionRef {
    /// Wrap an engine-native session identifier.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The engine-native identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors from a live backend.
#[derive(Debug, Error)]
pub enum LiveError {
    /// The engine could not be reached — callers should degrade to the durable log.
    #[error("live backend unavailable")]
    Unavailable,
    /// The engine rejected our credentials.
    #[error("live backend unauthorized: {0}")]
    Unauthorized(String),
    /// The session is unknown to the engine.
    #[error("session not found: {0}")]
    NotFound(String),
    /// The engine returned a non-success status.
    #[error("unexpected status {0}: {1}")]
    Status(u16, String),
    /// A transport or parse failure that is not a plain unavailability.
    #[error("transport: {0}")]
    Transport(String),
}

/// Control a live agent session. Implemented by engine adapters.
///
/// All methods are object-safe so call sites can hold a `Box<dyn Backend>` (or `Arc`) and
/// stay decoupled from the concrete engine.
pub trait Backend: Send + Sync {
    /// Deliver `text` into a live session (the **down** direction). Encoding the message
    /// is the caller's concern; the engine just delivers text.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the engine is down (the degrade signal).
    fn inject<'a>(
        &'a self,
        session: &'a SessionRef,
        text: &'a str,
    ) -> BoxFuture<'a, Result<(), LiveError>>;

    /// Stop the session's active execution; returns whether anything was running.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the engine is down (the degrade signal).
    fn interrupt<'a>(&'a self, session: &'a SessionRef) -> BoxFuture<'a, Result<bool, LiveError>>;

    /// Enumerate the engine's live sessions.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the engine is down (the degrade signal).
    fn active(&self) -> BoxFuture<'_, Result<Vec<SessionRef>, LiveError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ref_roundtrips() {
        let s = SessionRef::new("ses_abc");
        assert_eq!(s.as_str(), "ses_abc");
        assert_eq!(s.to_string(), "ses_abc");
        assert_eq!(s, SessionRef::new("ses_abc"));
    }
}
