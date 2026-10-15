//! End-to-end: an orchestrator and two children coordinate over `chan`.
//!
//! Flow: orchestrator tasks w1/w2 → each child asks a question → orchestrator
//! answers → each child returns a result → each child publishes progress to a
//! room the orchestrator reads. Exercises direct + room (pub/sub) + await.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;

fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_chan"))
        .arg("--dir")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn chan")
}

fn ok(out: Output) -> Output {
    assert!(
        out.status.success(),
        "chan failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

fn pairs(out: &Output) -> Vec<serde_json::Value> {
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    v.as_array().cloned().unwrap_or_default()
}

fn child(dir: PathBuf, name: &'static str) {
    // Wait for our task.
    ok(run(
        &dir,
        &["await", "--as", name, "--kind", "task", "--timeout", "10s"],
    ));
    // Publish progress to the shared room.
    ok(run(
        &dir,
        &[
            "publish",
            "--room",
            "build",
            "--from",
            name,
            "--kind",
            "progress",
            "--payload",
            r#""working""#,
        ],
    ));
    // Ask the orchestrator a question.
    ok(run(
        &dir,
        &[
            "send",
            "--from",
            name,
            "--to",
            "direct:orch",
            "--kind",
            "question",
            "--payload",
            r#""can I proceed?""#,
        ],
    ));
    // Wait for the answer.
    ok(run(
        &dir,
        &[
            "await",
            "--as",
            name,
            "--kind",
            "answer",
            "--timeout",
            "10s",
        ],
    ));
    // Return a result.
    ok(run(
        &dir,
        &[
            "send",
            "--from",
            name,
            "--to",
            "direct:orch",
            "--kind",
            "result",
            "--payload",
            r#"{"ok":true}"#,
        ],
    ));
}

#[test]
fn orchestrator_and_two_children() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    ok(run(&dir, &["init"]));

    let c1 = {
        let d = dir.clone();
        thread::spawn(move || child(d, "w1"))
    };
    let c2 = {
        let d = dir.clone();
        thread::spawn(move || child(d, "w2"))
    };

    thread::sleep(std::time::Duration::from_millis(300));

    // Dispatch tasks.
    for w in ["w1", "w2"] {
        ok(run(
            &dir,
            &[
                "send",
                "--from",
                "orch",
                "--to",
                &format!("direct:{w}"),
                "--kind",
                "task",
                "--payload",
                r#"{"phase":1}"#,
            ],
        ));
    }

    // Answer both questions, each to its actual sender.
    for _ in 0..2 {
        let q = ok(run(
            &dir,
            &[
                "await",
                "--as",
                "orch",
                "--kind",
                "question",
                "--timeout",
                "10s",
            ],
        ));
        let qv: serde_json::Value = serde_json::from_slice(&q.stdout).unwrap();
        let sender = qv["from"].as_str().unwrap().to_string();
        ok(run(
            &dir,
            &[
                "send",
                "--from",
                "orch",
                "--to",
                &format!("direct:{sender}"),
                "--kind",
                "answer",
                "--payload",
                r#""yes""#,
                "--corr",
                qv["id"].as_str().unwrap(),
            ],
        ));
    }

    c1.join().unwrap();
    c2.join().unwrap();

    // Orchestrator should now have two results.
    let inbox = ok(run(&dir, &["recv", "--as", "orch"]));
    let results: Vec<_> = pairs(&inbox)
        .into_iter()
        .filter(|m| m["kind"] == "result")
        .collect();
    assert_eq!(results.len(), 2, "expected two results");

    // The shared room should carry both progress updates.
    ok(run(&dir, &["subscribe", "--as", "orch", "--room", "build"]));
    let room = ok(run(&dir, &["room", "--as", "orch", "--room", "build"]));
    assert_eq!(pairs(&room).len(), 2, "expected two room messages");
}
