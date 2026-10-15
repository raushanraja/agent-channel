//! `OpencodeClient` as a [`chan_live::Backend`] adapter.
//!
//! All product-specific glue lives here: the port speaks plain text, and this module
//! translates it into opencode's wire format (`prompt_async` / `abort` / `session/status`).
//! Swapping engines means a new adapter crate — no call site changes.

use crate::client::OpencodeClient;
use chan_live::{Backend, BoxFuture, LiveError, SessionRef};

impl Backend for OpencodeClient {
    fn inject<'a>(
        &'a self,
        session: &'a SessionRef,
        text: &'a str,
    ) -> BoxFuture<'a, Result<(), LiveError>> {
        Box::pin(async move { self.prompt(session.as_str(), text).await })
    }

    fn interrupt<'a>(&'a self, session: &'a SessionRef) -> BoxFuture<'a, Result<bool, LiveError>> {
        Box::pin(async move { self.abort(session.as_str()).await })
    }

    fn active(&self) -> BoxFuture<'_, Result<Vec<SessionRef>, LiveError>> {
        Box::pin(async move {
            let ids = self.active_ids().await?;
            Ok(ids.into_iter().map(SessionRef::new).collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn inject_posts_the_text_as_a_part() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/session/ses_x/prompt"))
            .and(body_json(serde_json::json!({ "text": "more data" })))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "data": {} })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        c.inject(&SessionRef::new("ses_x"), "more data")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn active_maps_status_keys_to_session_refs() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/session/active"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "ses_a": { "type": "running" }, "ses_b": { "type": "running" } }
            })))
            .mount(&server)
            .await;
        let c = OpencodeClient::new(server.uri());
        let mut ids: Vec<String> = c
            .active()
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.as_str().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["ses_a", "ses_b"]);
    }
}
