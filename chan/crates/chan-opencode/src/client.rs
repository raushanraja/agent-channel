//! A minimal client for a running `opencode serve` instance (v2 HTTP API).
//!
//! Route shapes are pinned to the generated v2 SDK (`@opencode-ai/sdk/dist/v2`,
//! `types.gen.d.ts`), **not** guessed:
//!
//! | operation | route | body → response |
//! |---|---|---|
//! | inject | `POST /session/{id}/prompt_async` | `{parts:[{type:"text",text}]}` → `204` |
//! | interrupt | `POST /session/{id}/abort` | — → `200 bool` |
//! | status | `GET /session/status` | — → `{id: idle\|retry\|busy}` |
//! | history | `GET /session/{id}/message` | `?limit=` → `Message[]` |
//! | events | `GET /event` | SSE of `Event` JSON |
//!
//! Errors are the port's [`LiveError`] so this stays a [`chan_live::Backend`] adapter.
//! A connect failure maps to [`LiveError::Unavailable`] — the degrade signal.

use chan_live::LiveError;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use std::pin::Pin;

/// A stream of opencode server-sent events.
pub type EventStream =
    Pin<Box<dyn futures::Stream<Item = Result<eventsource_stream::Event, LiveError>> + Send>>;

fn classify(e: reqwest::Error) -> LiveError {
    if e.is_connect() {
        LiveError::Unavailable
    } else {
        LiveError::Transport(e.to_string())
    }
}

/// A handle to an opencode server's HTTP API.
#[derive(Clone)]
pub struct OpencodeClient {
    base: String,
    http: reqwest::Client,
}

impl OpencodeClient {
    /// Build a client for `base` (e.g. `http://127.0.0.1:4096`). Infallible.
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    /// The configured base URL.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Queue `text` into a session without blocking on the reply (`prompt_async`).
    /// This is the **inject** primitive — used both for new input and mid-flight steering.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the server is down; [`LiveError::NotFound`] for an
    /// unknown session.
    pub async fn prompt_async(&self, session: &str, text: &str) -> Result<(), LiveError> {
        let body = serde_json::json!({ "parts": [ { "type": "text", "text": text } ] });
        self.post_json(&format!("/session/{session}/prompt_async"), &body)
            .await?;
        Ok(())
    }

    /// Abort a session's active execution. Returns whether anything was running.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the server is down; [`LiveError::NotFound`] for an
    /// unknown session.
    pub async fn abort(&self, session: &str) -> Result<bool, LiveError> {
        let v = self
            .post_json(&format!("/session/{session}/abort"), &serde_json::json!({}))
            .await?;
        Ok(v.as_bool().unwrap_or(false))
    }

    /// The status of every session, keyed by session id.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the server is down.
    pub async fn status(&self) -> Result<serde_json::Map<String, serde_json::Value>, LiveError> {
        let v = self.get_json("/session/status").await?;
        Ok(v.as_object().cloned().unwrap_or_default())
    }

    /// Durable message history for a session.
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the server is down.
    pub async fn messages(
        &self,
        session: &str,
        limit: usize,
    ) -> Result<serde_json::Value, LiveError> {
        self.get_json(&format!("/session/{session}/message?limit={limit}"))
            .await
    }

    /// Subscribe to the server event stream (SSE).
    ///
    /// # Errors
    /// [`LiveError::Unavailable`] when the server is down.
    pub async fn event_stream(&self) -> Result<EventStream, LiveError> {
        let resp = self
            .http
            .get(self.url("/event"))
            .send()
            .await
            .map_err(classify)?;
        if !resp.status().is_success() {
            return Err(LiveError::Status(
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            ));
        }
        let stream = resp
            .bytes_stream()
            .eventsource()
            .map(|r| r.map_err(|e| LiveError::Transport(e.to_string())));
        Ok(Box::pin(stream))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, LiveError> {
        let resp = self
            .http
            .get(self.url(path))
            .send()
            .await
            .map_err(classify)?;
        decode(resp).await
    }

    async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, LiveError> {
        let resp = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(classify)?;
        decode(resp).await
    }
}

/// Turn a response into JSON, mapping non-success statuses to [`LiveError`].
async fn decode(resp: reqwest::Response) -> Result<serde_json::Value, LiveError> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return match status.as_u16() {
            404 => Err(LiveError::NotFound(body)),
            code => Err(LiveError::Status(code, body)),
        };
    }
    let bytes = resp.bytes().await.map_err(classify)?;
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    Ok(serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn prompt_async_posts_parts_and_accepts_204() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/session/ses_x/prompt_async"))
            .and(body_json(
                serde_json::json!({ "parts": [ { "type": "text", "text": "hi" } ] }),
            ))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        c.prompt_async("ses_x", "hi").await.unwrap();
    }

    #[tokio::test]
    async fn abort_posts_and_returns_bool() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/session/ses_x/abort"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(true)))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        assert!(c.abort("ses_x").await.unwrap());
    }

    #[tokio::test]
    async fn status_lists_sessions() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/session/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "ses_a": { "type": "busy" }, "ses_b": { "type": "idle" } }),
            ))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        let s = c.status().await.unwrap();
        assert_eq!(s.len(), 2);
    }

    #[tokio::test]
    async fn server_down_is_unavailable() {
        let c = OpencodeClient::new("http://127.0.0.1:1");
        let err = c.prompt_async("ses_x", "hi").await.unwrap_err();
        assert!(matches!(err, LiveError::Unavailable), "got {err:?}");
    }

    #[tokio::test]
    async fn unknown_session_is_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/session/missing/abort"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        let err = c.abort("missing").await.unwrap_err();
        assert!(matches!(err, LiveError::NotFound(_)), "got {err:?}");
    }
}
