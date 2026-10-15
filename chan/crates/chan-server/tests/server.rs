//! Integration tests: spawn the real axum server on an ephemeral port and hit it.

use std::sync::Arc;
use std::time::Duration;

use chan_core::{Address, FileBackend, Message, ParticipantId, RoomId};
use chan_server::ServerState;
use futures::StreamExt;

async fn spawn(token: Option<&str>) -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(ServerState {
        backend: FileBackend::init(tmp.path()).unwrap(),
        token: token.map(str::to_string),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = chan_server::router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp)
}

fn task(to: &str, body: serde_json::Value) -> serde_json::Value {
    serde_json::to_value(Message::task(
        ParticipantId::new("orch"),
        Address::Direct(ParticipantId::new(to)),
        body,
    ))
    .unwrap()
}

#[tokio::test]
async fn health_send_inbox_roundtrip() {
    let (base, _t) = spawn(None).await;
    let client = reqwest::Client::new();

    let health: serde_json::Value = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");

    let sent: serde_json::Value = client
        .post(format!("{base}/v1/send"))
        .json(&serde_json::json!({ "message": task("w1", serde_json::json!(1)) }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(sent["id"].is_string());

    let inbox: serde_json::Value = client
        .get(format!("{base}/v1/inbox/w1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["cursor"], 1);
}

#[tokio::test]
async fn room_subscribe_and_read() {
    let (base, _t) = spawn(None).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/v1/subscribe"))
        .json(&serde_json::json!({ "participant": "w1", "room": "build" }))
        .send()
        .await
        .unwrap();

    let m = serde_json::to_value(Message::task(
        ParticipantId::new("orch"),
        Address::Room(RoomId::new("build")),
        serde_json::json!("hi"),
    ))
    .unwrap();
    client
        .post(format!("{base}/v1/send"))
        .json(&serde_json::json!({ "message": m }))
        .send()
        .await
        .unwrap();

    let room: serde_json::Value = client
        .get(format!("{base}/v1/room/build?participant=w1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(room["messages"].as_array().unwrap().len(), 1);

    let rooms: serde_json::Value = client
        .get(format!("{base}/v1/rooms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        rooms["rooms"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r == "build")
    );
}

#[tokio::test]
async fn auth_is_enforced_but_health_is_open() {
    let (base, _t) = spawn(Some("secret")).await;
    let client = reqwest::Client::new();

    // health is open
    assert!(
        client
            .get(format!("{base}/v1/health"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );

    // no token -> 401
    let resp = client
        .post(format!("{base}/v1/send"))
        .json(&serde_json::json!({ "message": task("w1", serde_json::json!(1)) }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // wrong token -> 401
    let resp = client
        .post(format!("{base}/v1/send"))
        .bearer_auth("nope")
        .json(&serde_json::json!({ "message": task("w1", serde_json::json!(1)) }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // right token -> 200
    let resp = client
        .post(format!("{base}/v1/send"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "message": task("w1", serde_json::json!(1)) }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn sse_stream_delivers_a_new_message() {
    let (base, _t) = spawn(None).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/v1/inbox/w1/stream"))
        .send()
        .await
        .unwrap();

    let c2 = client.clone();
    let base2 = base.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        c2.post(format!("{base2}/v1/send"))
            .json(&serde_json::json!({ "message": task("w1", serde_json::json!(1)) }))
            .send()
            .await
            .unwrap();
    });

    let chunk = tokio::time::timeout(Duration::from_secs(5), resp.bytes_stream().next())
        .await
        .expect("stream timed out")
        .unwrap()
        .unwrap();
    let text = String::from_utf8_lossy(&chunk);
    assert!(text.contains("event: message"), "got: {text}");
    assert!(text.contains("data:"), "got: {text}");
}
