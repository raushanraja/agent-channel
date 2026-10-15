//! The message envelope.

use crate::address::{Address, ParticipantId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A time-sortable message identifier (UUIDv7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generate a fresh, time-sortable id.
    #[must_use]
    pub fn now() -> Self {
        Self(Uuid::now_v7())
    }

    /// The underlying UUID.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Parse an id from its canonical string form.
    pub fn parse(s: &str) -> crate::Result<Self> {
        Ok(Self(
            Uuid::parse_str(s).map_err(|e| crate::ChanError::Protocol(e.to_string()))?,
        ))
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The intent carried by a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Work to be done.
    Task,
    /// Progress update.
    Progress,
    /// A question expecting an answer.
    Question,
    /// An answer to a question.
    Answer,
    /// Final result of a task.
    Result,
    /// Acknowledgement of receipt.
    Ack,
    /// Control signal (interrupt/resume).
    Control,
    /// A domain event.
    Event,
}

/// An immutable channel message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Unique id.
    pub id: MessageId,
    /// Sender.
    pub from: ParticipantId,
    /// Destination.
    pub to: Address,
    /// Creation time (UTC).
    pub ts: DateTime<Utc>,
    /// Intent.
    pub kind: MessageKind,
    /// Threads a reply to its originating request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<MessageId>,
    /// The message this one directly replies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<MessageId>,
    /// Kind-specific body.
    pub payload: serde_json::Value,
    /// Optional time-to-live in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl Message {
    fn base(
        from: ParticipantId,
        to: Address,
        kind: MessageKind,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: MessageId::now(),
            from,
            to,
            ts: Utc::now(),
            kind,
            correlation_id: None,
            reply_to: None,
            payload,
            ttl_secs: None,
        }
    }

    /// Build a `Task` message.
    pub fn task(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Task, payload)
    }

    /// Build a `Progress` message.
    pub fn progress(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Progress, payload)
    }

    /// Build a `Question` message.
    pub fn question(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Question, payload)
    }

    /// Build an `Answer` message.
    pub fn answer(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Answer, payload)
    }

    /// Build a `Result` message.
    pub fn result(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Result, payload)
    }

    /// Build an `Ack` message.
    pub fn ack(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self {
        Self::base(from, to, MessageKind::Ack, payload)
    }

    /// Reply to this message, threading the correlation id.
    #[must_use]
    pub fn reply(
        &self,
        from: ParticipantId,
        kind: MessageKind,
        payload: serde_json::Value,
    ) -> Self {
        let mut m = Self::base(from, self.from.clone().into(), kind, payload);
        m.reply_to = Some(self.id);
        m.correlation_id = Some(self.correlation_id.unwrap_or(self.id));
        m
    }

    /// Whether this message has outlived its TTL at `now`.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        match self.ttl_secs {
            Some(ttl) => (now - self.ts).num_seconds().max(0) as u64 > ttl,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::RoomId;

    #[test]
    fn message_id_is_time_sortable() {
        let a = MessageId::now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = MessageId::now();
        assert!(a.as_uuid() < b.as_uuid());
    }

    #[test]
    fn task_envelope_carries_fields() {
        let m = Message::task(
            ParticipantId::new("orch"),
            Address::Room(RoomId::new("build")),
            serde_json::json!({"phase": 1}),
        );
        assert_eq!(m.kind, MessageKind::Task);
        assert_eq!(m.from.as_str(), "orch");
    }

    #[test]
    fn envelope_roundtrips_through_json() {
        let m = Message::question(
            ParticipantId::new("w1"),
            Address::Direct(ParticipantId::new("orch")),
            serde_json::json!("can I delete store.rs?"),
        );
        let s = serde_json::to_string(&m).unwrap();
        let back: Message = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn reply_threads_correlation() {
        let q = Message::question(
            ParticipantId::new("w1"),
            Address::Direct(ParticipantId::new("orch")),
            serde_json::json!("ok?"),
        );
        let a = q.reply(
            ParticipantId::new("orch"),
            MessageKind::Answer,
            serde_json::json!("yes"),
        );
        assert_eq!(a.reply_to, Some(q.id));
        assert_eq!(a.correlation_id, Some(q.id));
        assert_eq!(a.to, Address::Direct(ParticipantId::new("w1")));
    }

    #[test]
    fn ttl_expiry() {
        let mut m = Message::task(
            ParticipantId::new("a"),
            Address::Broadcast,
            serde_json::json!(1),
        );
        assert!(!m.is_expired(Utc::now()));
        m.ttl_secs = Some(0);
        m.ts = Utc::now() - chrono::Duration::seconds(5);
        assert!(m.is_expired(Utc::now()));
    }
}
