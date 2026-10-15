//! Durable-first, live-second attach: persist every message, then best-effort inject it
//! into a running session through any [`chan_live::Backend`].
//!
//! Engine-agnostic: it holds a `dyn Backend`, so opencode is one choice among many.

use chan_core::{ChanError, Channel, FileBackend, Message, MessageId};
use chan_live::{Backend, LiveError, SessionRef};
use std::path::Path;
use std::sync::Arc;

/// Binds the durable file log to a live backend.
pub struct AttachBackend {
    live: Arc<dyn Backend>,
    file: FileBackend,
}

impl AttachBackend {
    /// Open the file log at `chan_dir` and bind to `live`.
    ///
    /// # Errors
    /// Propagates [`ChanError`] if the channel directory cannot be created.
    pub fn new(live: Arc<dyn Backend>, chan_dir: impl AsRef<Path>) -> Result<Self, ChanError> {
        Ok(Self {
            live,
            file: FileBackend::init(chan_dir)?,
        })
    }

    /// The durable file backend.
    #[must_use]
    pub fn file(&self) -> &FileBackend {
        &self.file
    }

    /// The live backend.
    #[must_use]
    pub fn live(&self) -> &dyn Backend {
        self.live.as_ref()
    }

    /// Send a message: **durable first** (never lost), then best-effort live delivery to
    /// `session`. A missing engine degrades silently to the file log.
    ///
    /// # Errors
    /// Propagates [`ChanError`] only from the durable write; live failures are logged.
    pub async fn send(&self, session: &SessionRef, msg: &Message) -> Result<MessageId, ChanError> {
        let id = self.file.send(msg)?;
        let text = serde_json::to_string(msg).unwrap_or_default();
        match self.live.inject(session, &text).await {
            Ok(()) => tracing::debug!(session = %session, %id, "live delivery ok"),
            Err(LiveError::Unavailable) => tracing::warn!(
                session = %session,
                "live backend unavailable; message is durable in the file log"
            ),
            Err(e) => {
                tracing::warn!(session = %session, error = %e, "live delivery failed; message is durable");
            }
        }
        Ok(id)
    }

    /// Interrupt a live session's active execution (the **control** direction).
    ///
    /// # Errors
    /// Propagates [`LiveError`] from the backend.
    pub async fn interrupt(&self, session: &SessionRef) -> Result<bool, LiveError> {
        self.live.interrupt(session).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chan_core::{Address, ParticipantId};
    use chan_live::{BoxFuture, NoopBackend};
    use std::sync::Mutex;

    /// A backend that records injected text and reports success.
    #[derive(Clone, Default)]
    struct Recording {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl Backend for Recording {
        fn inject<'a>(
            &'a self,
            _session: &'a SessionRef,
            text: &'a str,
        ) -> BoxFuture<'a, Result<(), LiveError>> {
            Box::pin(async move {
                self.seen.lock().unwrap().push(text.to_string());
                Ok(())
            })
        }

        fn interrupt<'a>(
            &'a self,
            _session: &'a SessionRef,
        ) -> BoxFuture<'a, Result<bool, LiveError>> {
            Box::pin(async { Ok(false) })
        }

        fn active(&self) -> BoxFuture<'_, Result<Vec<SessionRef>, LiveError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn task_to(p: &ParticipantId) -> Message {
        Message::task(
            ParticipantId::new("orch"),
            Address::Direct(p.clone()),
            serde_json::json!("go"),
        )
    }

    #[tokio::test]
    async fn live_send_persists_then_injects() {
        let dir = tempfile::tempdir().unwrap();
        let rec = Recording::default();
        let handle = rec.clone();
        let ab = AttachBackend::new(Arc::new(rec), dir.path()).unwrap();
        let session = SessionRef::new("ses_x");
        let p = ParticipantId::new("w1");

        let id = ab.send(&session, &task_to(&p)).await.unwrap();

        let (msgs, _) = ab.file().recv_direct(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1, "message must be durable");
        let injected = handle.seen.lock().unwrap().clone();
        assert_eq!(injected.len(), 1, "live inject must fire once");
        let back: Message = serde_json::from_str(&injected[0]).unwrap();
        assert_eq!(back.id, id, "the injected text is the serialized message");
    }

    #[tokio::test]
    async fn unavailable_live_still_persists() {
        let dir = tempfile::tempdir().unwrap();
        let ab = AttachBackend::new(Arc::new(NoopBackend), dir.path()).unwrap();
        let session = SessionRef::new("ses_x");
        let p = ParticipantId::new("w1");

        ab.send(&session, &task_to(&p)).await.unwrap();

        let (msgs, _) = ab.file().recv_direct(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1, "degrade must still be durable");
    }

    #[tokio::test]
    async fn interrupt_delegates_to_backend() {
        let dir = tempfile::tempdir().unwrap();
        let ab = AttachBackend::new(Arc::new(Recording::default()), dir.path()).unwrap();
        assert!(!ab.interrupt(&SessionRef::new("ses_x")).await.unwrap());
    }
}
