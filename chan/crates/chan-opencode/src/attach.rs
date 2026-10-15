//! Durable-first, live-second attach: persist every message, then best-effort
//! inject it into a running opencode session.

use crate::client::{AttachError, OpencodeClient};
use chan_core::{Channel, FileBackend, Message, MessageId};
use std::path::Path;

/// Binds the durable file log to a live opencode server.
pub struct AttachBackend {
    client: OpencodeClient,
    file: FileBackend,
}

impl AttachBackend {
    /// Open the file log at `chan_dir` and bind to the server at `base`.
    pub fn new(base: impl Into<String>, chan_dir: impl AsRef<Path>) -> Result<Self, AttachError> {
        Ok(Self {
            client: OpencodeClient::new(base)?,
            file: FileBackend::init(chan_dir)?,
        })
    }

    /// The durable file backend.
    #[must_use]
    pub fn file(&self) -> &FileBackend {
        &self.file
    }

    /// The live client.
    #[must_use]
    pub fn client(&self) -> &OpencodeClient {
        &self.client
    }

    /// Send a message: **durable first** (never lost), then best-effort live
    /// delivery to `session`. A missing server degrades silently to the file log.
    pub async fn send(&self, session: &str, msg: &Message) -> Result<MessageId, AttachError> {
        let id = self.file.send(msg)?;
        let text = serde_json::to_string(msg).unwrap_or_default();
        match self.client.prompt(session, &text).await {
            Ok(()) => {
                tracing::debug!(session, %id, "live delivery ok");
            }
            Err(AttachError::ServerUnavailable) => {
                tracing::warn!(
                    session,
                    "opencode server unavailable; message is durable in the file log"
                );
            }
            Err(e) => {
                tracing::warn!(session, error = %e, "live delivery failed; message is durable");
            }
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chan_core::{Address, ParticipantId};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn task_to(p: &ParticipantId) -> Message {
        Message::task(
            ParticipantId::new("orch"),
            Address::Direct(p.clone()),
            serde_json::json!("go"),
        )
    }

    #[tokio::test]
    async fn live_send_persists_then_injects() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let ab = AttachBackend::new(server.uri(), dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        ab.send("ses_x", &task_to(&p)).await.unwrap();

        let (msgs, _) = ab.file().recv_direct(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1, "message must be durable");
    }

    #[tokio::test]
    async fn server_down_still_persists() {
        let dir = tempfile::tempdir().unwrap();
        let ab = AttachBackend::new("http://127.0.0.1:1", dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        // No server: send must still succeed (degrade to the file log).
        ab.send("ses_x", &task_to(&p)).await.unwrap();
        let (msgs, _) = ab.file().recv_direct(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
    }
}
