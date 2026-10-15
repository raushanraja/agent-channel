//! A minimal client for a running `opencode serve` instance (v2 HTTP API).
//!
//! Routes/shapes are pinned to the **running server's OpenAPI** (`GET /openapi.json`),
//! not to a stale SDK:
//!
//! | operation | route (under `/api`) | body → response |
//! |---|---|---|
//! | inject | `POST /session/{id}/prompt` | `{text}` → `{data:…}` |
//! | interrupt | `POST /session/{id}/interrupt` | — → `{interrupted:bool}` |
//! | active | `GET /session/active` | — → `{data:{id:…}}` |
//! | history | `GET /session/{id}/message` | `?limit=` → messages |
//! | events | `GET /event` | SSE of event JSON |
//!
//! The server mounts the API at `/api` and guards it with **HTTP Basic** auth
//! (`username: opencode`, password = the server password printed at startup). The
//! configured base is the server root (e.g. `http://127.0.0.1:4096`); `/api` is appended.
//!
//! Errors are the port's [`LiveError`]. A connect failure maps to
//! [`LiveError::Unavailable`] — the degrade signal.

use chan_live::LiveError;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use std::pin::Pin;

/// The fixed Basic-auth username the opencode server expects.
const AUTH_USER: &str = "opencode";

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
    api_base: String,
    password: Option<String>,
    http: reqwest::Client,
}

impl OpencodeClient {
    /// Build a client for a server root (e.g. `http://127.0.0.1:4096`), no auth.
    pub fn new(base: impl Into<String>) -> Self {
        Self::with_password(base, None)
    }

    /// Build a client with the server password (`OPENCODE_PASSWORD`).
    pub fn with_password(base: impl Into<String>, password: Option<String>) -> Self {
        let root = base.into();
        let root = root.trim_end_matches('/');
        let api_base = if root.ends_with("/api") {
            root.to_string()
        } else {
            format!("{root}/api")
        };
        Self {
            api_base,
            password,
            http: reqwest::Client::new(),
        }
    }

    /// The resolved API base (server root + `/api`).
    #[must_use]
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Queue `text` into a session (`POST /session/{id}/prompt`). This is the **inject**
    /// primitive — used both for new input and for adding data to a live session.
    ///
    /// # Errors
    /// [`LiveError::Unauthorized`] on bad credentials; [`LiveError::Unavailable`] when the
    /// server is down; [`LiveError::NotFound`] for an unknown session.
    pub async fn prompt(&self, session: &str, text: &str) -> Result<(), LiveError> {
        let body = serde_json::json!({ "text": text });
        self.post_json(&format!("/session/{session}/prompt"), &body)
            .await?;
        Ok(())
    }

    /// Interrupt a session's active execution (`POST /session/{id}/interrupt`). Returns
    /// whether anything was running.
    ///
    /// # Errors
    /// [`LiveError::Unauthorized`] on bad credentials; [`LiveError::Unavailable`] when the
    /// server is down.
    pub async fn abort(&self, session: &str) -> Result<bool, LiveError> {
        let v = self
            .post_json(
                &format!("/session/{session}/interrupt"),
                &serde_json::json!({}),
            )
            .await?;
        Ok(v.get("interrupted")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false))
    }

    /// The ids of the server's active sessions (`GET /session/active`).
    ///
    /// # Errors
    /// [`LiveError::Unauthorized`] on bad credentials; [`LiveError::Unavailable`] when the
    /// server is down.
    pub async fn active_ids(&self) -> Result<Vec<String>, LiveError> {
        let v = self.get_json("/session/active").await?;
        let data = v
            .get("data")
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        Ok(data.keys().cloned().collect())
    }

    /// Durable message history for a session.
    ///
    /// # Errors
    /// [`LiveError::Unauthorized`] on bad credentials; [`LiveError::Unavailable`] when the
    /// server is down.
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
    /// [`LiveError::Unauthorized`] on bad credentials; [`LiveError::Unavailable`] when the
    /// server is down.
    pub async fn event_stream(&self) -> Result<EventStream, LiveError> {
        let resp = self
            .auth(self.http.get(self.url("/event")))
            .send()
            .await
            .map_err(classify)?;
        if !resp.status().is_success() {
            return Err(status_error(resp).await);
        }
        let stream = resp
            .bytes_stream()
            .eventsource()
            .map(|r| r.map_err(|e| LiveError::Transport(e.to_string())));
        Ok(Box::pin(stream))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.api_base)
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.password {
            Some(pw) => rb.basic_auth(AUTH_USER, Some(pw)),
            None => rb,
        }
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, LiveError> {
        let resp = self
            .auth(self.http.get(self.url(path)))
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
            .auth(self.http.post(self.url(path)).json(body))
            .send()
            .await
            .map_err(classify)?;
        decode(resp).await
    }
}

/// Build an error for a non-success response, mapping 401/404 specially.
async fn status_error(resp: reqwest::Response) -> LiveError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    match status.as_u16() {
        401 => LiveError::Unauthorized(body),
        404 => LiveError::NotFound(body),
        code => LiveError::Status(code, body),
    }
}

/// Turn a response into JSON, mapping non-success statuses to [`LiveError`].
async fn decode(resp: reqwest::Response) -> Result<serde_json::Value, LiveError> {
    if !resp.status().is_success() {
        return Err(status_error(resp).await);
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
    use wiremock::matchers::{body_json, header_regex, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn prompt_posts_text_to_api_prompt_with_basic_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .and(header_regex("authorization", "^Basic "))
            .and(body_json(serde_json::json!({ "text": "hi" })))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "data": {} })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let c = OpencodeClient::with_password(server.uri(), Some("pw".into()));
        c.prompt("ses_x", "hi").await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_reads_interrupted_flag() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/interrupt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "interrupted": true })),
            )
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        assert!(c.abort("ses_x").await.unwrap());
    }

    #[tokio::test]
    async fn active_lists_session_ids_from_data() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/session/active"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({ "data": { "ses_a": { "type": "running" } } }),
                ),
            )
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        assert_eq!(c.active_ids().await.unwrap(), vec!["ses_a"]);
    }

    #[tokio::test]
    async fn unauthorized_is_classified() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .respond_with(ResponseTemplate::new(401).set_body_json(
                serde_json::json!({ "_tag": "UnauthorizedError", "message": "Authentication required" }),
            ))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        let err = c.prompt("ses_x", "hi").await.unwrap_err();
        assert!(matches!(err, LiveError::Unauthorized(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unknown_session_is_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/missing/interrupt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        let err = c.abort("missing").await.unwrap_err();
        assert!(matches!(err, LiveError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn server_down_is_unavailable() {
        let c = OpencodeClient::new("http://127.0.0.1:1");
        let err = c.prompt("ses_x", "hi").await.unwrap_err();
        assert!(matches!(err, LiveError::Unavailable), "got {err:?}");
    }

    #[test]
    fn base_url_gets_api_suffix_once() {
        assert_eq!(
            OpencodeClient::new("http://h:4096").api_base(),
            "http://h:4096/api"
        );
        assert_eq!(
            OpencodeClient::new("http://h:4096/").api_base(),
            "http://h:4096/api"
        );
        assert_eq!(
            OpencodeClient::new("http://h:4096/api").api_base(),
            "http://h:4096/api"
        );
    }
}
