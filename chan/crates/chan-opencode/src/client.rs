//! A minimal client for a running `opencode serve` instance.

use eventsource_stream::Eventsource;
use futures::StreamExt;
use std::pin::Pin;
use thiserror::Error;

/// Errors from the opencode attach layer.
#[derive(Debug, Error)]
pub enum AttachError {
    /// The HTTP transport failed.
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    /// The server could not be reached — callers should degrade to the file log.
    #[error("opencode server unavailable")]
    ServerUnavailable,
    /// The server returned a non-success status.
    #[error("unexpected status {0}: {1}")]
    Status(u16, String),
    /// Server-sent-events parsing failed.
    #[error("sse: {0}")]
    Sse(String),
    /// The underlying channel failed.
    #[error("chan: {0}")]
    Core(#[from] chan_core::ChanError),
}

fn classify(e: reqwest::Error) -> AttachError {
    if e.is_connect() {
        AttachError::ServerUnavailable
    } else {
        AttachError::Http(e)
    }
}

/// A handle to an opencode server's HTTP API.
#[derive(Clone)]
pub struct OpencodeClient {
    base: String,
    http: reqwest::Client,
}

impl OpencodeClient {
    /// Build a client for `base` (e.g. `http://127.0.0.1:4096`).
    pub fn new(base: impl Into<String>) -> Result<Self, AttachError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(AttachError::Http)?;
        Ok(Self {
            base: base.into(),
            http,
        })
    }

    /// Inject `text` as a queued prompt into a session.
    pub async fn prompt(&self, session: &str, text: &str) -> Result<(), AttachError> {
        self.post(
            &format!("/api/session/{session}/prompt"),
            &serde_json::json!({ "text": text }),
        )
        .await?;
        Ok(())
    }

    /// Inject `text` as a mid-flight steering message (delivery = `steer`).
    pub async fn steer(&self, session: &str, text: &str) -> Result<(), AttachError> {
        self.post(
            &format!("/api/session/{session}/prompt"),
            &serde_json::json!({ "text": text, "delivery": "steer" }),
        )
        .await?;
        Ok(())
    }

    /// Add a synthetic (non-LLM-visible-in-the-usual-way) note to a session.
    pub async fn synthetic(&self, session: &str, text: &str) -> Result<(), AttachError> {
        self.post(
            &format!("/api/session/{session}/synthetic"),
            &serde_json::json!({ "text": text }),
        )
        .await?;
        Ok(())
    }

    /// Interrupt a session's active execution.
    pub async fn interrupt(&self, session: &str, resume: bool) -> Result<bool, AttachError> {
        let v = self
            .post(
                &format!("/api/session/{session}/interrupt?resume={resume}"),
                &serde_json::json!({}),
            )
            .await?;
        Ok(v.get("interrupted")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false))
    }

    /// List a session's messages (durable history).
    pub async fn messages(
        &self,
        session: &str,
        limit: usize,
    ) -> Result<serde_json::Value, AttachError> {
        let resp = self
            .http
            .get(format!(
                "{}/api/session/{session}/message?limit={limit}",
                self.base
            ))
            .send()
            .await
            .map_err(classify)?;
        if !resp.status().is_success() {
            return Err(AttachError::Status(
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            ));
        }
        Ok(resp.json().await?)
    }

    /// List active sessions.
    pub async fn active(&self) -> Result<serde_json::Value, AttachError> {
        let resp = self
            .http
            .get(format!("{}/api/session/active", self.base))
            .send()
            .await
            .map_err(classify)?;
        if !resp.status().is_success() {
            return Err(AttachError::Status(
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            ));
        }
        Ok(resp.json().await?)
    }

    /// Subscribe to the server event stream (SSE).
    pub async fn event_stream(
        &self,
    ) -> Result<
        Pin<Box<dyn futures::Stream<Item = Result<eventsource_stream::Event, AttachError>> + Send>>,
        AttachError,
    > {
        let resp = self
            .http
            .get(format!("{}/api/event", self.base))
            .send()
            .await
            .map_err(classify)?;
        let stream = resp
            .bytes_stream()
            .eventsource()
            .map(|r| r.map_err(|e| AttachError::Sse(e.to_string())));
        Ok(Box::pin(stream))
    }

    async fn post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, AttachError> {
        let resp = self
            .http
            .post(format!("{}{path}", self.base))
            .json(body)
            .send()
            .await
            .map_err(classify)?;
        if !resp.status().is_success() {
            return Err(AttachError::Status(
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            ));
        }
        let bytes = resp.bytes().await?;
        if bytes.is_empty() {
            return Ok(serde_json::json!({}));
        }
        Ok(serde_json::from_slice(&bytes).unwrap_or_else(|_| serde_json::json!({})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn prompt_posts_text_to_session_prompt() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .and(body_json(serde_json::json!({ "text": "hello" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri()).unwrap();
        c.prompt("ses_x", "hello").await.unwrap();
    }

    #[tokio::test]
    async fn steer_sets_delivery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .and(body_json(
                serde_json::json!({ "text": "stop", "delivery": "steer" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri()).unwrap();
        c.steer("ses_x", "stop").await.unwrap();
    }

    #[tokio::test]
    async fn server_down_is_classified() {
        let c = OpencodeClient::new("http://127.0.0.1:1").unwrap();
        let err = c.prompt("ses_x", "hi").await.unwrap_err();
        assert!(matches!(err, AttachError::ServerUnavailable), "got {err:?}");
    }
}
