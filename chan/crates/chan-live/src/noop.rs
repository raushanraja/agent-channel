//! A backend whose engine is permanently down — forces the durable-only path in tests.

use crate::port::{Backend, BoxFuture, LiveError, SessionRef};

/// A [`Backend`] that always reports [`LiveError::Unavailable`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopBackend;

impl Backend for NoopBackend {
    fn inject<'a>(
        &'a self,
        _session: &'a SessionRef,
        _text: &'a str,
    ) -> BoxFuture<'a, Result<(), LiveError>> {
        Box::pin(async { Err(LiveError::Unavailable) })
    }

    fn interrupt<'a>(&'a self, _session: &'a SessionRef) -> BoxFuture<'a, Result<bool, LiveError>> {
        Box::pin(async { Err(LiveError::Unavailable) })
    }

    fn active(&self) -> BoxFuture<'_, Result<Vec<SessionRef>, LiveError>> {
        Box::pin(async { Err(LiveError::Unavailable) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_is_always_unavailable() {
        let b = NoopBackend;
        let s = SessionRef::new("ses_x");
        assert!(matches!(
            b.inject(&s, "go").await,
            Err(LiveError::Unavailable)
        ));
        assert!(matches!(b.interrupt(&s).await, Err(LiveError::Unavailable)));
        assert!(matches!(b.active().await, Err(LiveError::Unavailable)));
    }
}
