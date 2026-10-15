//! End-to-end tests for the `chan` binary (spawns the real process).

use std::path::Path;
use std::process::{Command, Output};

fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_chan"))
        .arg("--dir")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn chan")
}

#[test]
fn send_then_recv_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());

    let out = run(
        tmp.path(),
        &[
            "send",
            "--from",
            "orch",
            "--to",
            "direct:w1",
            "--kind",
            "task",
            "--payload",
            r#"{"x":1}"#,
        ],
    );
    assert!(
        out.status.success(),
        "send failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = run(tmp.path(), &["recv", "--as", "w1"]);
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1);
    assert_eq!(v[0]["kind"], "task");
}

#[test]
fn await_times_out_with_exit_3() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());
    let out = run(tmp.path(), &["await", "--as", "nobody", "--timeout", "1s"]);
    assert_eq!(out.status.code(), Some(3));
}

#[test]
fn room_publish_then_read() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());
    assert!(
        run(tmp.path(), &["subscribe", "--as", "w1", "--room", "build"])
            .status
            .success()
    );
    assert!(
        run(
            tmp.path(),
            &[
                "publish",
                "--room",
                "build",
                "--from",
                "orch",
                "--kind",
                "progress",
                "--payload",
                r#""hi""#,
            ],
        )
        .status
        .success()
    );
    let out = run(tmp.path(), &["room", "--as", "w1", "--room", "build"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1);
    assert_eq!(v[0]["kind"], "progress");
}

#[test]
fn await_returns_matching_message() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());

    // A background producer sends a question shortly after.
    let dir = tmp.path().to_path_buf();
    let producer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        run(
            &dir,
            &[
                "send",
                "--from",
                "orch",
                "--to",
                "direct:w1",
                "--kind",
                "question",
                "--payload",
                r#""ok?""#,
            ],
        );
    });

    let out = run(
        tmp.path(),
        &[
            "await",
            "--as",
            "w1",
            "--kind",
            "question",
            "--timeout",
            "5s",
        ],
    );
    producer.join().unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["kind"], "question");
}

#[test]
fn attach_once_degrades_without_a_live_server() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());
    assert!(
        run(
            tmp.path(),
            &[
                "send",
                "--from",
                "orch",
                "--to",
                "direct:w1",
                "--kind",
                "task",
                "--payload",
                r#"{"x":1}"#,
            ],
        )
        .status
        .success()
    );

    // No opencode server on :1 — the bridge must still drain durably and exit 0.
    let out = run(
        tmp.path(),
        &[
            "attach",
            "--participant",
            "w1",
            "--session",
            "ses_x",
            "--once",
            "--opencode",
            "http://127.0.0.1:1",
        ],
    );
    assert!(
        out.status.success(),
        "attach failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The inbox was consumed (cursor advanced) despite the live inject degrading.
    let out = run(tmp.path(), &["inbox", "--as", "w1"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["unread"], 0);
}

#[test]
fn interrupt_without_a_live_server_fails_loudly() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(run(tmp.path(), &["init"]).status.success());
    let out = run(
        tmp.path(),
        &[
            "interrupt",
            "--session",
            "ses_x",
            "--opencode",
            "http://127.0.0.1:1",
        ],
    );
    assert!(
        !out.status.success(),
        "interrupt should fail when the engine is down"
    );
}
