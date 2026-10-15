//! The `Channel` backend abstraction.

use crate::Result;
use crate::address::{ParticipantId, RoomId};
use crate::message::{Message, MessageId};
use serde::{Deserialize, Serialize};

/// A read position, counted in messages consumed.
pub type Cursor = u64;

/// Summary counts for a channel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelStats {
    /// Number of participant inboxes.
    pub participants: usize,
    /// Number of room logs.
    pub rooms: usize,
}

/// A pluggable message backend.
///
/// Implementations must be safe for concurrent use from multiple processes:
/// [`FileBackend`](crate::FileBackend) achieves this with per-file locks.
pub trait Channel {
    /// Append a message; returns its id.
    fn send(&self, msg: &Message) -> Result<MessageId>;

    /// Read up to `limit` messages from `who`'s direct inbox, starting at `cursor`.
    /// Returns the messages and the next cursor.
    fn recv(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)>;

    /// Read up to `limit` messages from `room`'s log for `who`, starting at `cursor`.
    /// Fan-out is exactly-once per subscriber because `cursor` is caller-held.
    fn recv_room(
        &self,
        who: &ParticipantId,
        room: &RoomId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)>;

    /// Ensure `who` is subscribed to `room`; returns their current cursor.
    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor>;

    /// Record acknowledgement of `id` by `who`.
    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()>;

    /// Summary counts.
    fn list(&self) -> Result<ChannelStats>;
}
