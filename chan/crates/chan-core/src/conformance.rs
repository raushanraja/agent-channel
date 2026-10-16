//! A reusable conformance suite every [`Channel`] implementation should pass.
//!
//! Gated to test builds for now; promote behind a feature when another crate
//! needs to run it against its own backend.

use crate::address::{Address, ParticipantId, RoomId};
use crate::backend::Channel;
use crate::message::{Message, MessageKind};

/// Run the full conformance suite against a freshly-built channel.
///
/// `new` returns the channel plus a `TempDir` holder that must outlive it.
pub fn run<C: Channel>(new: impl Fn() -> (C, tempfile::TempDir)) {
    direct_roundtrip(&new);
    room_fanout_exactly_once(&new);
    room_echo_suppressed(&new);
    ack_idempotent(&new);
    ttl_filtered(&new);
    threading_preserved(&new);
}

fn direct_roundtrip<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let p = ParticipantId::new("orch");
    c.send(&Message::task(
        ParticipantId::new("w1"),
        Address::Direct(p.clone()),
        serde_json::json!(1),
    ))
    .unwrap();
    let (m, cur) = c.recv(&p, 0, 10).unwrap();
    assert_eq!(m.len(), 1);
    assert_eq!(cur, 1);
}

fn room_fanout_exactly_once<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let room = RoomId::new("build");
    let a = ParticipantId::new("a");
    let cur = c.subscribe(&a, &room).unwrap();
    c.send(&Message::task(
        ParticipantId::new("o"),
        Address::Room(room.clone()),
        serde_json::json!(1),
    ))
    .unwrap();
    let (m, cur2) = c.recv_room(&a, &room, cur, 10).unwrap();
    assert_eq!(m.len(), 1);
    let (m2, _) = c.recv_room(&a, &room, cur2, 10).unwrap();
    assert!(m2.is_empty(), "a subscriber must not see a message twice");
}

fn ack_idempotent<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let p = ParticipantId::new("orch");
    let id = c
        .send(&Message::task(
            ParticipantId::new("w1"),
            Address::Direct(p.clone()),
            serde_json::json!(1),
        ))
        .unwrap();
    c.ack(&p, &id).unwrap();
    c.ack(&p, &id).unwrap();
}

fn ttl_filtered<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let p = ParticipantId::new("orch");
    let mut m = Message::task(
        ParticipantId::new("w1"),
        Address::Direct(p.clone()),
        serde_json::json!(1),
    );
    m.ttl_secs = Some(0);
    m.ts = chrono::Utc::now() - chrono::Duration::seconds(5);
    c.send(&m).unwrap();
    let (got, _) = c.recv(&p, 0, 10).unwrap();
    assert!(got.is_empty(), "an expired message must be filtered");
}

fn threading_preserved<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let p = ParticipantId::new("orch");
    let q = Message::question(
        ParticipantId::new("w1"),
        Address::Direct(p.clone()),
        serde_json::json!("ok?"),
    );
    let a = q.reply(
        ParticipantId::new("orch"),
        MessageKind::Answer,
        serde_json::json!("yes"),
    );
    assert_eq!(a.correlation_id, Some(q.id));
    c.send(&a).unwrap();
    let (got, _) = c.recv(&ParticipantId::new("w1"), 0, 10).unwrap();
    assert_eq!(got[0].correlation_id, Some(q.id));
}

pub fn room_echo_suppressed<C: Channel>(new: &impl Fn() -> (C, tempfile::TempDir)) {
    let (c, _d) = new();
    let room = RoomId::new("team");
    let w1 = ParticipantId::new("w1");
    let w2 = ParticipantId::new("w2");
    c.subscribe(&w1, &room).unwrap();
    c.subscribe(&w2, &room).unwrap();

    // w1 publishes to the room
    c.send(&Message::task(
        w1.clone(),
        Address::Room(room.clone()),
        serde_json::json!("hello from w1"),
    ))
    .unwrap();

    // w1 reading the room must NOT see its own message
    let (msgs_w1, _) = c.recv_room(&w1, &room, 0, 10).unwrap();
    assert!(
        msgs_w1.is_empty(),
        "w1 must not see its own room publication"
    );

    // w2 reading the room MUST see w1's message
    let (msgs_w2, _) = c.recv_room(&w2, &room, 0, 10).unwrap();
    assert_eq!(msgs_w2.len(), 1, "w2 must see w1's publication");
    assert_eq!(msgs_w2[0].from, w1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_backend::FileBackend;

    #[test]
    fn file_backend_passes_conformance() {
        run(|| {
            let d = tempfile::tempdir().unwrap();
            let c = FileBackend::init(d.path()).unwrap();
            (c, d)
        });
    }
}
