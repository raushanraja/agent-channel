//! Drives the MCP server over real stdio: initialize, tools/list, tools/call.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::Duration;

fn spawn(dir: &std::path::Path) -> (Child, ChildStdin, Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_chan-mcp"))
        .arg("--dir")
        .arg(dir)
        .arg("--as")
        .arg("orch")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn chan-mcp");
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    (child, stdin, rx)
}

fn send(stdin: &mut ChildStdin, v: &serde_json::Value) {
    writeln!(stdin, "{v}").unwrap();
    stdin.flush().unwrap();
}

fn recv_until(rx: &Receiver<String>, needle: &str) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                if line.contains(needle) {
                    return line;
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                panic!("timed out waiting for `{needle}`");
            }
        }
    }
}

#[test]
fn initialize_list_and_call_send() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut child, mut stdin, rx) = spawn(tmp.path());

    send(
        &mut stdin,
        &serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "itest", "version": "0" }
            }
        }),
    );
    let init = recv_until(&rx, "\"serverInfo\"");
    assert!(init.contains("\"chan\""), "unexpected init: {init}");

    send(
        &mut stdin,
        &serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    );

    send(
        &mut stdin,
        &serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
    );
    let tools = recv_until(&rx, "\"id\":2");
    for name in ["chan_send", "chan_recv", "chan_await", "chan_publish"] {
        assert!(tools.contains(name), "missing tool {name} in {tools}");
    }

    send(
        &mut stdin,
        &serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "chan_send",
                "arguments": { "to": "direct:w1", "kind": "task", "payload": { "x": 1 } }
            }
        }),
    );
    let call = recv_until(&rx, "\"id\":3");
    assert!(!call.contains("\"isError\":true"), "call errored: {call}");

    // The durable log must hold the message.
    let inbox = std::fs::read_to_string(tmp.path().join("participants/w1.jsonl")).unwrap();
    assert!(inbox.contains("\"kind\":\"task\""), "inbox: {inbox}");

    let _ = child.kill();
}
