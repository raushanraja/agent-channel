//! Network E2E: the `chan` CLI talking to a real `chan-serve` daemon, with auth.

use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use chan_core::FileBackend;
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

fn run(state: &Path, server: &str, token: Option<&str>, args: &[&str]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_chan"));
    c.arg("--server").arg(server);
    c.arg("--state").arg(state);
    if let Some(t) = token {
        c.arg("--token").arg(t);
    }
    c.args(args);
    c.output().expect("spawn chan")
}

fn ok(out: Output) -> Output {
    assert!(
        out.status.success(),
        "chan failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

#[test]
fn network_exchange_with_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("cursors.json");
    let base = start_server(&tmp.path().join("srv"), Some("sekret"));

    // Unauthenticated send must be refused (server 401 -> client error, exit 1).
    let bad = run(
        &state,
        &base,
        None,
        &[
            "send",
            "--from",
            "orch",
            "--to",
            "direct:w1",
            "--kind",
            "task",
            "--payload",
            "1",
        ],
    );
    assert!(!bad.status.success(), "unauth send should fail");

    // Authenticated exchange over the network.
    ok(run(
        &state,
        &base,
        Some("sekret"),
        &[
            "send",
            "--from",
            "orch",
            "--to",
            "direct:w1",
            "--kind",
            "task",
            "--payload",
            r#"{"phase":1}"#,
        ],
    ));
    let got = ok(run(
        &state,
        &base,
        Some("sekret"),
        &["await", "--as", "w1", "--kind", "task", "--timeout", "5s"],
    ));
    let v: serde_json::Value = serde_json::from_slice(&got.stdout).unwrap();
    assert_eq!(v["kind"], "task");

    ok(run(
        &state,
        &base,
        Some("sekret"),
        &[
            "send",
            "--from",
            "w1",
            "--to",
            "direct:orch",
            "--kind",
            "result",
            "--payload",
            r#"{"ok":true}"#,
        ],
    ));
    let inbox = ok(run(
        &state,
        &base,
        Some("sekret"),
        &["recv", "--as", "orch"],
    ));
    let msgs: Vec<serde_json::Value> = serde_json::from_slice(&inbox.stdout).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["kind"], "result");
}
