#![forbid(unsafe_code)]
//! `chan-client` — a [`Channel`] implementation that talks to a `chan-serve` daemon.
//!
//! Blocking by design, so the sync `Channel` trait (and the CLI) stay unchanged.

use chan_core::{
    ChanError, Channel, ChannelStats, Cursor, Message, MessageId, ParticipantId, Result, RoomId,
};
use reqwest::blocking::{Client, RequestBuilder};
use serde_json::Value;

/// A channel backed by a remote `chan-serve` daemon.
pub struct RemoteBackend {
    base: String,
    token: Option<String>,
    http: Client,
}

fn transport(e: reqwest::Error) -> ChanError {
    ChanError::Transport(e.to_string())
}

impl RemoteBackend {
    /// Point at a daemon, optionally authenticating with `token`.
    #[must_use]
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            token,
            http: Client::new(),
        }
    }

    fn auth(&self, rb: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn decode(resp: reqwest::blocking::Response) -> Result<Value> {
        let status = resp.status();
        let text = resp
            .text()
            .map_err(|e| ChanError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(ChanError::Transport(format!("{status}: {text}")));
        }
        serde_json::from_str(&text).map_err(ChanError::Serde)
    }

    fn page(v: &Value) -> Result<(Vec<Message>, Cursor)> {
        let msgs = v
            .get("messages")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let cursor = v.get("cursor").and_then(Value::as_u64).unwrap_or(0);
        Ok((msgs, cursor))
    }
}

impl Channel for RemoteBackend {
    fn send(&self, msg: &Message) -> Result<MessageId> {
        let resp = self
            .auth(self.http.post(self.url("/v1/send")))
            .json(&serde_json::json!({ "message": msg }))
            .send()
            .map_err(transport)?;
        let v = Self::decode(resp)?;
        let id = v
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ChanError::Protocol("response missing id".into()))?;
        MessageId::parse(id)
    }

    fn recv(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        let resp = self
            .auth(
                self.http
                    .get(self.url(&format!("/v1/inbox/{who}?cursor={cursor}&limit={limit}"))),
            )
            .send()
            .map_err(transport)?;
        let v = Self::decode(resp)?;
        Self::page(&v)
    }

    fn recv_room(
        &self,
        who: &ParticipantId,
        room: &RoomId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        let resp = self
            .auth(self.http.get(self.url(&format!(
                "/v1/room/{room}?participant={who}&cursor={cursor}&limit={limit}"
            ))))
            .send()
            .map_err(transport)?;
        let v = Self::decode(resp)?;
        Self::page(&v)
    }

    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor> {
        let resp = self
            .auth(self.http.post(self.url("/v1/subscribe")))
            .json(&serde_json::json!({ "participant": who.as_str(), "room": room.as_str() }))
            .send()
            .map_err(transport)?;
        let v = Self::decode(resp)?;
        Ok(v.get("cursor").and_then(Value::as_u64).unwrap_or(0))
    }

    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()> {
        let resp = self
            .auth(self.http.post(self.url("/v1/ack")))
            .json(&serde_json::json!({ "participant": who.as_str(), "id": id.to_string() }))
            .send()
            .map_err(transport)?;
        Self::decode(resp)?;
        Ok(())
    }

    fn list(&self) -> Result<ChannelStats> {
        let resp = self
            .auth(self.http.get(self.url("/v1/health")))
            .send()
            .map_err(transport)?;
        let v = Self::decode(resp)?;
        Ok(ChannelStats {
            participants: v.get("participants").and_then(Value::as_u64).unwrap_or(0) as usize,
            rooms: v.get("rooms").and_then(Value::as_u64).unwrap_or(0) as usize,
        })
    }
}
