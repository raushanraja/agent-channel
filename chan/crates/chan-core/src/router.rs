//! Address resolution over a [`Channel`].

use crate::Result;
use crate::address::{ParticipantId, RoomId};
use crate::backend::{Channel, ChannelStats, Cursor};
use crate::message::{Message, MessageId};

/// Resolves [`Address`](crate::Address)es against a [`Channel`] and exposes the
/// channel protocol as a single facade.
pub struct Router<C: Channel> {
    channel: C,
}

impl<C: Channel> Router<C> {
    /// Wrap a channel.
    pub fn new(channel: C) -> Self {
        Self { channel }
    }

    /// The underlying channel.
    #[must_use]
    pub fn channel(&self) -> &C {
        &self.channel
    }

    /// Send a message (direct, room, or broadcast).
    pub fn send(&self, msg: &Message) -> Result<MessageId> {
        self.channel.send(msg)
    }

    /// Read a participant's direct inbox.
    pub fn recv(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        self.channel.recv(who, cursor, limit)
    }

    /// Read a room log for a subscriber.
    pub fn recv_room(
        &self,
        who: &ParticipantId,
        room: &RoomId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        self.channel.recv_room(who, room, cursor, limit)
    }

    /// Subscribe to a room.
    pub fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor> {
        self.channel.subscribe(who, room)
    }

    /// Acknowledge a message.
    pub fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()> {
        self.channel.ack(who, id)
    }

    /// Summary counts.
    pub fn list(&self) -> Result<ChannelStats> {
        self.channel.list()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::Address;
    use crate::file_backend::FileBackend;

    #[test]
    fn router_broadcast_hits_the_all_room() {
        let dir = tempfile::tempdir().unwrap();
        let r = Router::new(FileBackend::init(dir.path()).unwrap());
        let p = ParticipantId::new("w1");
        let cur = r.subscribe(&p, &RoomId::new("all")).unwrap();
        r.send(&Message::progress(
            ParticipantId::new("w2"),
            Address::Broadcast,
            serde_json::json!("halfway"),
        ))
        .unwrap();
        let (msgs, _) = r.recv_room(&p, &RoomId::new("all"), cur, 10).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn router_direct_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let r = Router::new(FileBackend::init(dir.path()).unwrap());
        let p = ParticipantId::new("orch");
        r.send(&Message::task(
            ParticipantId::new("w1"),
            Address::Direct(p.clone()),
            serde_json::json!("go"),
        ))
        .unwrap();
        let (msgs, c) = r.recv(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(c, 1);
    }
}
