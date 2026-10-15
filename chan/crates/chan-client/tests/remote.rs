//! `RemoteBackend` against a real `chan-server` daemon (started on a background thread).

use std::path::Path;
use std::sync::Arc;

use chan_client::RemoteBackend;
use chan_core::{Address, Channel, FileBackend, Message, ParticipantId, RoomId};
use chan_server::ServerState;

fn start_server(dir: &Path, token: Option<&str>) -> String {
    let backend = FileBackend::init(dir).unwrap();
    let token = token.map(str::to_string);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let state = Arc::new(ServerState { backend, token });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap()).unwrap();
            axum::serve(listener, chan_server::router(state))
                .await
                .unwrap();
        });
    });
    format!("http://{}", rx.recv().unwrap())
}

fn task(to: &str) -> Message {
    Message::task(
        ParticipantId::new("orch"),
        Address::Direct(ParticipantId::new(to)),
        serde_json::json!(1),
    )
}

#[test]
fn remote_channel_conformance() {
    let tmp = tempfile::tempdir().unwrap();
    let base = start_server(tmp.path(), None);
    let c = RemoteBackend::new(base, None);

    // direct roundtrip
    let who = ParticipantId::new("w1");
    let id = c.send(&task("w1")).unwrap();
    let (msgs, cur) = c.recv(&who, 0, 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].id, id);
    assert_eq!(cur, 1);

    // ack idempotent
    c.ack(&who, &id).unwrap();
    c.ack(&who, &id).unwrap();

    // room fan-out exactly-once via caller-held cursors
    let room = RoomId::new("build");
    let a = ParticipantId::new("a");
    let cur = c.subscribe(&a, &room).unwrap();
    let m = Message::task(
        ParticipantId::new("orch"),
        Address::Room(room.clone()),
        serde_json::json!("hi"),
    );
    c.send(&m).unwrap();
    let (r1, cur2) = c.recv_room(&a, &room, cur, 10).unwrap();
    assert_eq!(r1.len(), 1);
    let (r2, _) = c.recv_room(&a, &room, cur2, 10).unwrap();
    assert!(r2.is_empty(), "no re-delivery");

    // stats
    let stats = c.list().unwrap();
    assert!(stats.participants >= 1);
}

#[test]
fn remote_auth_required() {
    let tmp = tempfile::tempdir().unwrap();
    let base = start_server(tmp.path(), Some("secret"));

    // No token -> transport error (401).
    let anon = RemoteBackend::new(base.clone(), None);
    assert!(anon.send(&task("w1")).is_err());

    // Correct token -> works.
    let ok = RemoteBackend::new(base, Some("secret".to_string()));
    ok.send(&task("w1")).unwrap();
}
